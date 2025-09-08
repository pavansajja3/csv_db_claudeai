import os
import pandas as pd
import mysql.connector
from flask import Flask, request, render_template, jsonify
from mysql.connector import Error
import logging
import time
from werkzeug.utils import secure_filename

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500MB max file size

# Ensure uploads folder exists
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

# ✅ Cleaned DB connection (no pool)
def get_db_connection():
    return mysql.connector.connect(
        host="xxxxxx",          # ✅ Use IP or short hostname
        user="pavan",
        password="xxxxxxx", # ❗ Replace with your actual password
        database="xxxxxxx",
        autocommit=False,
        use_unicode=True,
        charset='utf8mb4',
        buffered=True,
        connection_timeout=60,
        sql_mode='TRADITIONAL',
        raise_on_warnings=False
    )

# CSV validation
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'

# Main upload and insert logic
def create_table_from_csv_optimized(csv_file, table_name="voter1"):
    start_time = time.time()
    
    try:
        logger.info(f"Starting CSV processing: {csv_file}")
        df = pd.read_csv(
            csv_file, 
            dtype=str, 
            keep_default_na=False,
            na_values=[''],
            skip_blank_lines=True,
            encoding='utf-8'
        )

        logger.info(f"CSV loaded with {len(df)} rows and {len(df.columns)} columns")

        if "voter_id" not in df.columns:
            raise Exception("CSV must contain 'voter_id' column")

        # Clean column names
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('[^a-zA-Z0-9_]', '', regex=True)
        voter_id_col = [col for col in df.columns if 'voter_id' in col.lower()][0]
        df = df[df[voter_id_col].str.strip() != ""]
        df = df.dropna(subset=[voter_id_col])

        if df.empty:
            raise Exception("No valid data rows found after filtering")

        logger.info(f"After filtering: {len(df)} valid rows")

        db = get_db_connection()
        cursor = db.cursor()

        try:
            # Build and create table
            columns = []
            for col in df.columns:
                if col == voter_id_col:
                    columns.append(f"`{col}` VARCHAR(255) PRIMARY KEY")
                else:
                    max_length = df[col].astype(str).str.len().max()
                    col_length = min(max(max_length, 50), 1000)
                    columns.append(f"`{col}` VARCHAR({col_length})")

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            ) ENGINE=InnoDB 
              DEFAULT CHARSET=utf8mb4 
              COLLATE=utf8mb4_unicode_ci
              ROW_FORMAT=DYNAMIC
            """
            cursor.execute(create_table_query)
            logger.info(f"Table {table_name} created successfully")

            # Prepare insert
            column_names = ', '.join([f'`{col}`' for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"""
            INSERT INTO {table_name} ({column_names}) 
            VALUES ({placeholders})
            """

            # Convert data
            data_tuples = []
            for _, row in df.iterrows():
                values = []
                for val in row.values:
                    if pd.isna(val) or val == '' or str(val).strip().lower() in ['nan', 'null', 'none']:
                        values.append(None)
                    else:
                        values.append(str(val).strip()[:1000])
                data_tuples.append(tuple(values))

            logger.info(f"Prepared {len(data_tuples)} rows for insertion")

            # Bulk insert
            batch_size = 5000
            total_batches = (len(data_tuples) + batch_size - 1) // batch_size

            for i, batch_start in enumerate(range(0, len(data_tuples), batch_size)):
                batch_end = min(batch_start + batch_size, len(data_tuples))
                batch = data_tuples[batch_start:batch_end]
                cursor.executemany(insert_query, batch)

                if (i + 1) % 5 == 0 or i == total_batches - 1:
                    db.commit()
                    logger.info(f"Committed batch {i+1}/{total_batches}")

            db.commit()

            try:
                cursor.execute(f"CREATE INDEX idx_{table_name}_voter_id ON {table_name} (`{voter_id_col}`)")
            except Error:
                pass

            processing_time = round(time.time() - start_time, 2)
            logger.info(f"Inserted {len(data_tuples)} rows in {processing_time} seconds")
            return len(data_tuples), processing_time

        except Error as e:
            db.rollback()
            logger.error(f"Database error: {str(e)}")
            raise e
        finally:
            cursor.close()
            db.close()

    except Exception as e:
        logger.error(f"Error processing CSV: {str(e)}")
        raise e
    finally:
        if os.path.exists(csv_file):
            os.remove(csv_file)
            logger.info(f"Deleted file: {csv_file}")

# Flask routes
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        try:
            if "file" not in request.files:
                return jsonify({"error": "No file uploaded"}), 400

            file = request.files["file"]
            if file.filename == "":
                return jsonify({"error": "No selected file"}), 400

            if not allowed_file(file.filename):
                return jsonify({"error": "Please upload a CSV file"}), 400

            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            file.save(filepath)
            logger.info(f"File saved: {filepath}")

            row_count, processing_time = create_table_from_csv_optimized(filepath, table_name="voter1")

            return jsonify({
                "success": True,
                "message": f"✅ CSV uploaded successfully! {row_count} rows inserted in {processing_time} seconds",
                "rows_inserted": row_count,
                "processing_time": processing_time
            })

        except Exception as e:
            logger.error(str(e))
            return jsonify({"error": f"❌ Error: {str(e)}"}), 500

    return render_template("index.html")

@app.route("/health")
def health_check():
    return jsonify({"status": "healthy", "timestamp": time.time()})

@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": "File too large. Maximum size is 500MB."}), 413

# Run app
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
