# test_import.py
try:
    import config
    print("config imported")
except Exception as e:
    print(f"config error: {e}")

try:
    import db
    print("db imported")
except Exception as e:
    print(f"db error: {e}")

if hasattr(db, 'Database'):
    print("Database class exists")
else:
    print("Database class not found")