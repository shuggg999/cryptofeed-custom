#!/usr/bin/env python3
"""
Check PostgreSQL table structures and constraints to identify potential issues.
"""
import psycopg2
from psycopg2.extras import RealDictCursor

postgres_cfg = {
    'host': '127.0.0.1.txt',
    'user': 'postgres',
    'database': 'cryptofeed',
    'password': 'password'
}

def check_table_structure():
    """Check the structure of all relevant tables"""

    try:
        conn = psycopg2.connect(**postgres_cfg)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        tables_to_check = ['trades', 'candles', 'funding', 'ticker']

        for table in tables_to_check:
            print(f"\n{'='*60}")
            print(f"🔍 TABLE: {table}")
            print(f"{'='*60}")

            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                );
            """, (table,))

            exists = cursor.fetchone()['exists']

            if not exists:
                print(f"❌ Table '{table}' does not exist!")
                continue

            print(f"✅ Table '{table}' exists")

            # Get table structure
            cursor.execute("""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                ORDER BY ordinal_position;
            """, (table,))

            columns = cursor.fetchall()
            print(f"\n📋 Columns ({len(columns)}):")
            for col in columns:
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                length = f"({col['character_maximum_length']})" if col['character_maximum_length'] else ""
                default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
                print(f"  - {col['column_name']}: {col['data_type']}{length} {nullable}{default}")

            # Check constraints
            cursor.execute("""
                SELECT
                    constraint_name,
                    constraint_type
                FROM information_schema.table_constraints
                WHERE table_schema = 'public'
                AND table_name = %s;
            """, (table,))

            constraints = cursor.fetchall()
            if constraints:
                print(f"\n🔗 Constraints ({len(constraints)}):")
                for constraint in constraints:
                    print(f"  - {constraint['constraint_name']}: {constraint['constraint_type']}")

            # Check indexes
            cursor.execute("""
                SELECT
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE tablename = %s
                AND schemaname = 'public';
            """, (table,))

            indexes = cursor.fetchall()
            if indexes:
                print(f"\n📇 Indexes ({len(indexes)}):")
                for index in indexes:
                    print(f"  - {index['indexname']}")
                    print(f"    {index['indexdef']}")

            # Check if it's partitioned
            cursor.execute("""
                SELECT
                    partrelid::regclass AS partition_name,
                    pg_get_expr(partexprs, partrelid) AS partition_expression
                FROM pg_partitioned_table pt
                JOIN pg_inherits i ON pt.partrelid = i.inhparent
                WHERE pt.partrelid = %s::regclass;
            """, (table,))

            partitions = cursor.fetchall()
            if partitions:
                print(f"\n🔀 Partitions ({len(partitions)}):")
                for partition in partitions:
                    print(f"  - {partition['partition_name']}: {partition['partition_expression']}")
            else:
                print(f"\n📄 Not partitioned")

            # Check current row count
            cursor.execute(f"SELECT COUNT(*) as count FROM {table};")
            count = cursor.fetchone()['count']
            print(f"\n📊 Current row count: {count:,}")

            # Check recent records if any exist
            if count > 0:
                cursor.execute(f"""
                    SELECT * FROM {table}
                    ORDER BY timestamp DESC
                    LIMIT 3;
                """)
                recent = cursor.fetchall()
                print(f"\n🕒 Recent records (last 3):")
                for i, record in enumerate(recent, 1):
                    print(f"  {i}. {dict(record)}")

        conn.close()

    except Exception as e:
        print(f"❌ Database error: {e}")
        import traceback
        traceback.print_exc()

def check_database_connection():
    """Test basic database connectivity"""
    print("🔌 Testing database connection...")

    try:
        conn = psycopg2.connect(**postgres_cfg)
        cursor = conn.cursor()

        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ Connected to: {version}")

        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()[0]
        print(f"✅ Current database: {db_name}")

        cursor.execute("SELECT current_user;")
        user = cursor.fetchone()[0]
        print(f"✅ Current user: {user}")

        conn.close()
        return True

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def main():
    print("🔍 PostgreSQL Table Structure Analysis")
    print("="*60)

    if check_database_connection():
        check_table_structure()
    else:
        print("❌ Cannot proceed without database connection")

if __name__ == '__main__':
    main()