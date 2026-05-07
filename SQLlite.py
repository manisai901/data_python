import sqlite3


def save_to_sql(topic, explanation):
    # This creates a file named 'learning_history.db' in your folder
    conn = sqlite3.connect(os.path.join(current_dir, 'batch_3.db'))
    cursor = conn.cursor()

    # Create a table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS t1 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT,
            explanation TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Insert the AI data
    cursor.execute('INSERT INTO t1 (topic, explanation) VALUES (?, ?)', (topic, explanation))

    conn.commit()
    conn.close()
    print("✅ Data successfully saved to SQL table 'study_notes'!")

# To use it, just call save_to_sql(topic, explanation) inside your learn function!