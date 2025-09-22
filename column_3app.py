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
    """Ensure CSV columns exist, rename if needed, expand lengths."""
    cursor.execute("""
        SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
    """, (table_name,))
    existing_cols_info = cursor.fetchall()
    existing = {row[0]: row[1] for row in existing_cols_info}
    existing_cols = list(existing.keys())
    csv_cols = list(df.columns)

    max_lengths = {col: min(max(df[col].astype(str).str.len().max(), 50), 1000) for col in csv_cols}

    for csv_col in csv_cols:
        if csv_col not in existing_cols:
            candidates = [c for c in existing_cols if c not in csv_cols and c != voter_id_col]
            if candidates:
                best_match = difflib.get_close_matches(csv_col, candidates, n=1, cutoff=0.75)
                if best_match:
                    old_col = best_match[0]
                    cursor.execute(
                        f"ALTER TABLE {table_name} CHANGE `{old_col}` `{csv_col}` VARCHAR({max_lengths[csv_col]})"
                    )
                    logger.info(f"Renamed column: {old_col} -> {csv_col} (len={max_lengths[csv_col]})")
                    existing[csv_col] = max_lengths[csv_col]
                    existing.pop(old_col)
                    existing_cols = list(existing.keys())

    # Check if table has primary key
    cursor.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND CONSTRAINT_TYPE = 'PRIMARY KEY'
    """, (table_name,))
    has_pk = cursor.fetchone()[0] > 0

    # Add missing columns or expand existing
    for col in csv_cols:
        needed_len = max_lengths[col]
        if col not in existing:
            if col == voter_id_col and not has_pk:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN `{col}` VARCHAR(255) PRIMARY KEY")
                has_pk = True
            else:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN `{col}` VARCHAR({needed_len})")
            logger.info(f"Added missing column: {col} (len={needed_len})")
            existing[col] = needed_len
        else:
            current_len = existing[col] if existing[col] else 50
            if current_len < needed_len:
                cursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN `{col}` VARCHAR({needed_len})")
                logger.info(f"Expanded column: {col} from {current_len} to {needed_len}")
                existing[col] = needed_len

def bulk_insert_or_update(db, cursor, table_name, df, voter_id_col, batch_size=5000):
    """Bulk insert/update CSV data."""
    column_names = ', '.join([f'`{col}`' for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))
    update_clause = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in df.columns if col != voter_id_col])

    insert_query = f"""
        INSERT INTO {table_name} ({column_names})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
    """

    data_tuples = []
    for _, row in df.iterrows():
        values = [str(val).strip()[:1000] if val and str(val).strip().lower() not in ['nan','null','none'] else None for val in row.values]
        data_tuples.append(tuple(values))

    total_batches = (len(data_tuples) + batch_size - 1) // batch_size
    for i, batch_start in enumerate(range(0, len(data_tuples), batch_size)):
        batch_end = min(batch_start + batch_size, len(data_tuples))
        batch = data_tuples[batch_start:batch_end]
        cursor.executemany(insert_query, batch)
        if (i + 1) % 5 == 0 or i == total_batches - 1:
            db.commit()  # Commit using connection, not cursor
            logger.info(f"Committed batch {i+1}/{total_batches}")

    return len(data_tuples)

def create_table_from_csv_optimized(csv_file, table_name="voter3"):
    start_time = time.time()
    try:
        logger.info(f"Processing CSV: {csv_file}")
        df = pd.read_csv(csv_file, dtype=str, keep_default_na=False, na_values=[''], skip_blank_lines=True, encoding='utf-8')
        logger.info(f"CSV loaded with {len(df)} rows and {len(df.columns)} columns")

        if "voter_id" not in df.columns:
            raise Exception("CSV must contain 'voter_id' column")

        df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('[^a-zA-Z0-9_]', '', regex=True)
        voter_id_col = [col for col in df.columns if 'voter_id' in col.lower()][0]
        df = df[df[voter_id_col].str.strip() != ""].dropna(subset=[voter_id_col])

        if df.empty:
            raise Exception("No valid data rows found after filtering")

        db = get_db_connection()
        cursor = db.cursor()

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
        logger.info(f"Table {table_name} created or exists")

        ensure_columns_exist_and_rename(cursor, table_name, df, voter_id_col)
        rows_inserted = bulk_insert_or_update(db, cursor, table_name, df, voter_id_col)

        try:
            cursor.execute(f"CREATE INDEX idx_{table_name}_voter_id ON {table_name} (`{voter_id_col}`)")
        except Error:
            pass

        processing_time = round(time.time() - start_time, 2)
        logger.info(f"Inserted/Updated {rows_inserted} rows in {processing_time} seconds")
        return rows_inserted, processing_time

    except Exception as e:
        logger.error(f"Error processing CSV: {str(e)}")
        raise e
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'db' in locals():
            db.close()
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
