import sqlite3
import sys

DB_FILE = "tasks.db"


def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        description TEXT NOT NULL,
        done INTEGER DEFAULT 0
    )
    """)

    conn.commit()
    conn.close()


def add_task(description):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("INSERT INTO tasks (description) VALUES (?)", (description,))
    conn.commit()

    conn.close()
    print("Tarefa adicionada.")


def list_tasks():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("SELECT id, description, done FROM tasks")
    rows = cursor.fetchall()

    conn.close()

    for r in rows:
        status = "✔" if r[2] else " "
        print(f"{r[0]}. [{status}] {r[1]}")


def complete_task(task_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("UPDATE tasks SET done = 1 WHERE id = ?", (task_id,))
    conn.commit()

    conn.close()
    print("Tarefa concluída.")


def delete_task(task_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
    conn.commit()

    conn.close()
    print("Tarefa removida.")


def main():
    init_db()

    if len(sys.argv) < 2:
        print("Comandos: add, list, done, delete")
        return

    command = sys.argv[1]

    if command == "add":
        description = " ".join(sys.argv[2:])
        add_task(description)

    elif command == "list":
        list_tasks()

    elif command == "done":
        complete_task(int(sys.argv[2]))

    elif command == "delete":
        delete_task(int(sys.argv[2]))

    else:
        print("Comando inválido.")


if __name__ == "__main__":
    main()
