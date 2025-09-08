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

# Database connection with optimized settings
def get_db_connection():
    return mysql.connector.connect(
        host="digi-signage.c560k8amypvh.ap-south-1.rds.amazonaws.com",
        user="pavan",
        password="Welcome*1234",
        database="vote_track",
        autocommit=False,  # We'll handle commits manually
        use_unicode=True,
        charset='utf8mb4',
        buffered=True,
        connection_timeout=60,
        pool_size=65,
        pool_reset_session=True,
        sql_mode='TRADITIONAL',
        raise_on_warnings=False
    )

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'

def create_table_from_csv_optimized(csv_file, table_name="csv_datayog"):
    start_time = time.time()
    
    try:
        logger.info(f"Starting CSV processing: {csv_file}")
        
        # Read CSV with optimized pandas settings
        df = pd.read_csv(
            csv_file, 
            dtype=str, 
            keep_default_na=False,
            na_values=[''],
            skip_blank_lines=True,
            encoding='utf-8'
        )
        
        logger.info(f"CSV loaded with {len(df)} rows and {len(df.columns)} columns")
        
        # Ensure voter_id column exists
        if "voter_id" not in df.columns:
            raise Exception("CSV must contain 'voter_id' column")
        
        # Clean column names (remove extra spaces, special characters)
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('[^a-zA-Z0-9_]', '', regex=True)
        
        # Update voter_id column name if it was cleaned
        voter_id_col = [col for col in df.columns if 'voter_id' in col.lower()][0]
        
        # Remove rows with empty voter_id
        df = df[df[voter_id_col].str.strip() != ""]
        df = df.dropna(subset=[voter_id_col])
        
        if df.empty:
            raise Exception("No valid data rows found after filtering")
        
        logger.info(f"After filtering: {len(df)} valid rows")
        
        db = get_db_connection()
        cursor = db.cursor()
        
        try:
            # Build table schema with appropriate data types
            columns = []
            for col in df.columns:
                if col == voter_id_col:
                    columns.append(f"`{col}` VARCHAR(255) PRIMARY KEY")
                else:
                    # Determine appropriate column length based on data
                    max_length = df[col].astype(str).str.len().max()
                    col_length = min(max(max_length, 50), 1000)  # Between 50-1000 chars
                    columns.append(f"`{col}` VARCHAR({col_length})")
            
            # Drop table if exists (for clean insert)
           # cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create table with optimized settings
            create_table_query = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns)}
            ) ENGINE=InnoDB 
              DEFAULT CHARSET=utf8mb4 
              COLLATE=utf8mb4_unicode_ci
              ROW_FORMAT=DYNAMIC
            """
            cursor.execute(create_table_query)
            logger.info(f"Table {table_name} created successfully")
            
            # Optimize MySQL settings for bulk insert
            optimization_queries = [
                "SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'",
                "SET SESSION foreign_key_checks = 0",
                "SET SESSION unique_checks = 0",
                "SET SESSION autocommit = 0",
                "SET SESSION innodb_buffer_pool_size = 2147483648",  # 2GB if available
                "SET SESSION bulk_insert_buffer_size = 268435456",   # 256MB
                "SET SESSION myisam_sort_buffer_size = 268435456"    # 256MB
            ]
            
            for query in optimization_queries:
                try:
                    cursor.execute(query)
                except Error:
                    pass  # Some settings might not be adjustable in managed databases
            
            # Prepare bulk insert query with LOAD DATA LOCAL INFILE alternative
            column_names = ', '.join([f'`{col}`' for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"""
            INSERT INTO {table_name} ({column_names}) 
            VALUES ({placeholders})
            """
            
            # Convert DataFrame to list of tuples for bulk insert
            data_tuples = []
            for _, row in df.iterrows():
                values = []
                for val in row.values:
                    if pd.isna(val) or val == '' or str(val).strip().lower() in ['nan', 'null', 'none']:
                        values.append(None)
                    else:
                        values.append(str(val).strip()[:1000])  # Truncate if too long
                data_tuples.append(tuple(values))
            
            logger.info(f"Prepared {len(data_tuples)} rows for insertion")
            
            # Bulk insert with optimal batch size
            batch_size = 5000  # Larger batch size for better performance
            total_batches = (len(data_tuples) + batch_size - 1) // batch_size
            
            for i, batch_start in enumerate(range(0, len(data_tuples), batch_size)):
                batch_end = min(batch_start + batch_size, len(data_tuples))
                batch = data_tuples[batch_start:batch_end]
                
                cursor.executemany(insert_query, batch)
                
                # Commit every few batches to prevent timeout
                if (i + 1) % 5 == 0 or i == total_batches - 1:
                    db.commit()
                    logger.info(f"Completed batch {i+1}/{total_batches}")
            
            # Final commit
            db.commit()
            
            # Reset MySQL settings
            reset_queries = [
                "SET SESSION foreign_key_checks = 1",
                "SET SESSION unique_checks = 1",
                "SET SESSION autocommit = 1"
            ]
            
            for query in reset_queries:
                try:
                    cursor.execute(query)
                except Error:
                    pass
            
            # Create indexes for better query performance
            try:
                cursor.execute(f"CREATE INDEX idx_{table_name}_voter_id ON {table_name} (`{voter_id_col}`)")
            except Error:
                pass  # Index might already exist
            
            end_time = time.time()
            processing_time = round(end_time - start_time, 2)
            
            logger.info(f"Successfully inserted {len(data_tuples)} rows in {processing_time} seconds")
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
        # Delete uploaded file after processing
        if os.path.exists(csv_file):
            os.remove(csv_file)
            logger.info(f"Cleaned up file: {csv_file}")

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        try:
            # Validate file upload
            if "file" not in request.files:
                return jsonify({"error": "No file uploaded"}), 400
            
            file = request.files["file"]
            if file.filename == "":
                return jsonify({"error": "No selected file"}), 400
            
            if not allowed_file(file.filename):
                return jsonify({"error": "Please upload a CSV file"}), 400
            
            # Secure filename
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            
            # Save file
            file.save(filepath)
            logger.info(f"File saved: {filepath}")
            
            # Process CSV
            row_count, processing_time = create_table_from_csv_optimized(filepath, table_name="csv_datayog")
            
            success_message = f"✅ CSV uploaded successfully! {row_count} rows inserted in {processing_time} seconds"
            
            return jsonify({
                "success": True,
                "message": success_message,
                "rows_inserted": row_count,
                "processing_time": processing_time
            })
            
        except Exception as e:
            error_message = f"❌ Error: {str(e)}"
            logger.error(error_message)
            return jsonify({"error": error_message}), 500
    
    return render_template("index.html")

@app.route("/health")
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": time.time()})

@app.errorhandler(413)
def too_large(e):
    return jsonify({"error": "File too large. Maximum size is 500MB."}), 413

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)