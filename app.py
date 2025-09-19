from flask import Flask, render_template, request, jsonify
import mysql.connector

import database

app = Flask(__name__)

# Configuração do banco
db_config = database.config()


def get_connection():
    return mysql.connector.connect(**db_config)


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/url", methods=["GET", "POST"])
def url_form():
    if request.method == "POST":
        url = request.form.get("base_url")
        repo = request.form.get("repository_name")

        # grava no banco
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO oai_identify (repository_name, base_url) VALUES (%s, %s)",
            (repo, url))
        conn.commit()
        conn.close()

        flash("Repositório adicionado com sucesso!", "success")
        return redirect(url_for("identify"))

    return render_template("url.html")

@app.route("/identify")
def identify():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM oai_identify")
    data = cursor.fetchall()
    conn.close()
    return render_template("identify.html", identify=data)


@app.route("/records")
def records():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM oai_records LIMIT 50")
    data = cursor.fetchall()
    conn.close()
    return render_template("records.html", records=data)


@app.route("/records/<int:rec_id>")
def get_record(rec_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM oai_records WHERE id = %s", (rec_id, ))
    data = cursor.fetchone()
    conn.close()
    return render_template("record.html", record=data)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
