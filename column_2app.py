import os
import pandas as pd
import mysql.connector
from flask import Flask, request, render_template, jsonify
from mysql.connector import Error
import logging
import time
from werkzeug.utils import secure_filename
import difflib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500MB

# Ensure uploads folder exists
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="pavan@1997",
        database="vote",
        autocommit=False,
        use_unicode=True,
        charset='utf8mb4',
        buffered=True,
        connection_timeout=60,
        sql_mode='TRADITIONAL',
        raise_on_warnings=False
    )

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'

def ensure_columns_exist_and_rename(cursor, table_name, df, voter_id_col):
    """Ensure all CSV columns exist, are long enough, and handle renamed columns."""
    # Get existing columns with their current length
    cursor.execute(f"""
        SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
    """, (table_name,))
    existing_cols_info = cursor.fetchall()
    existing = {row[0]: row[1] for row in existing_cols_info}

    existing_cols = list(existing.keys())
    csv_cols = list(df.columns)

    # Detect possible renames using name similarity
    for csv_col in csv_cols:
        if csv_col not in existing_cols:
            # Try to find a similar old column name not present in CSV anymore
            candidates = [c for c in existing_cols if c not in csv_cols and c != voter_id_col]
            if candidates:
                best_match = difflib.get_close_matches(csv_col, candidates, n=1, cutoff=0.75)
                if best_match:
                    old_col = best_match[0]
                    # Rename column in MySQL
                    max_length = int(df[csv_col].astype(str).str.len().max())
                    new_length = min(max(max_length, 50), 1000)
                    cursor.execute(
                        f"ALTER TABLE {table_name} CHANGE `{old_col}` `{csv_col}` VARCHAR({new_length})"
                    )
                    logger.info(f"Renamed column: {old_col} -> {csv_col} (len={new_length})")
                    existing[csv_col] = new_length
                    existing.pop(old_col, None)
                    existing_cols = list(existing.keys())

    # Add any missing columns
    for col in csv_cols:
        max_length = int(df[col].astype(str).str.len().max())
        needed_length = min(max(max_length, 50), 1000)

        if col not in existing:
            if col == voter_id_col:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN `{col}` VARCHAR(255) PRIMARY KEY")
            else:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN `{col}` VARCHAR({needed_length})")
            logger.info(f"Added missing column: {col} (len={needed_length})")
            existing[col] = needed_length
        else:
            # Expand if needed
            current_len = existing[col] if existing[col] else 50
            if current_len < needed_length:
                cursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN `{col}` VARCHAR({needed_length})")
                logger.info(f"Expanded column: {col} from {current_len} to {needed_length}")

def create_table_from_csv_optimized(csv_file, table_name="voter3"):
    start_time = time.time()
    
    try:
        logger.info(f"Starting CSV processing: {csv_file}")
        df = pd.read_csv(csv_file, dtype=str, keep_default_na=False, na_values=[''], skip_blank_lines=True, encoding='utf-8')

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
            # Step 1: Create table if it doesn't exist
            columns = []
            for col in df.columns:
                if col == voter_id_col:
                    columns.append(f"`{col}` VARCHAR(255) PRIMARY KEY")
                else:
                    max_length = int(df[col].astype(str).str.len().max())
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
            logger.info(f"Table {table_name} created or already exists")

            # Step 2: Handle renames, add missing, and expand if needed
            ensure_columns_exist_and_rename(cursor, table_name, df, voter_id_col)

            # Step 3: Prepare insert with conditional update
            column_names = ', '.join([f'`{col}`' for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            update_clause = ', '.join([
                f"`{col}` = IF(VALUES(`{col}`) != `{col}`, VALUES(`{col}`), `{col}`)"
                for col in df.columns if col != voter_id_col
            ])

            insert_query = f"""
            INSERT INTO {table_name} ({column_names}) 
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
            """

            # Step 4: Convert data
            data_tuples = []
            for _, row in df.iterrows():
                values = []
                for val in row.values:
                    if pd.isna(val) or val == '' or str(val).strip().lower() in ['nan', 'null', 'none']:
                        values.append(None)
                    else:
                        values.append(str(val).strip()[:1000])
                data_tuples.append(tuple(values))

            logger.info(f"Prepared {len(data_tuples)} rows for insert/update")

            # Step 5: Bulk insert in batches
            batch_size = 5000
            total_batches = (len(data_tuples) + batch_size - 1) // batch_size

            for i, batch_start in enumerate(range(0, len(data_tuples), batch_size)):
                batch_end = min(batch_start + batch_size, len(data_tuples))
                batch = data_tuples[batch_start:batch_end]
                cursor.executemany(insert_query, batch)

                if (i + 1) % 5 == 0 or i == total_batches - 1:
                    db.commit()
                    logger.info(f"Committed batch {i+1}/{total_batches}")

            # Step 6: Create index on voter_id column (ignore if exists)
            try:
                cursor.execute(f"CREATE INDEX idx_{table_name}_voter_id ON {table_name} (`{voter_id_col}`)")
            except Error:
                pass

            processing_time = round(time.time() - start_time, 2)
            logger.info(f"Inserted/Updated {len(data_tuples)} rows in {processing_time} seconds")
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

            row_count, processing_time = create_table_from_csv_optimized(filepath, table_name="voter3")

            return jsonify({
                "success": True,
                "message": f"✅ CSV uploaded successfully! {row_count} rows inserted/updated in {processing_time} seconds",
                "rows_inserted_or_updated": row_count,
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

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
