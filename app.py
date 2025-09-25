from flask import Flask, render_template, request, jsonify, redirect, url_for, flash

import mysql.connector

import database
import oai_ListSets
import oai_identify
import oai_listRecords

app = Flask(__name__)
app.secret_key = "minha_chave_super_secreta"  # NECESSÁRIO para usar flash


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
        url = request.form.get("base_url") or ""
        repo = request.form.get("repository_name") or ""

        # --- 1. Sanitiza a URL (remove query params ? e &)
        url = url.split("?")[0].split("&")[0].strip()

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # --- 2. Verifica se já existe essa URL
        cursor.execute("SELECT id FROM oai_identify WHERE base_url = %s", (url,))
        existing = cursor.fetchone()

        if existing:
            flash("⚠️ Essa URL já está cadastrada!", "warning")
        else:
            # --- 3. Insere no banco
            cursor.execute(
                "INSERT INTO oai_identify (repository_name, base_url) VALUES (%s, %s)",
                (repo, url)
            )
            conn.commit()
            flash("✅ Repositório adicionado com sucesso!", "success")

        conn.close()
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

@app.route("/listsets/<int:repo_id>")
def getListSets(repo_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM oai_identify WHERE id = %s", (repo_id,))
    repo = cursor.fetchone()

    if repo and not repo.get("repository_name"):
        oai_identify.update_identify(repo)
        cursor.execute("SELECT * FROM oai_identify WHERE id = %s", (repo_id,))
        repo = cursor.fetchone()

    # coleta sets e salva no banco
    oai_ListSets.coletar_list_sets(base_url=repo['base_url'], identify_id=repo_id)

    # carrega sets já armazenados
    cursor.execute("SELECT * FROM oai_sets WHERE identify_id = %s", (repo_id,))
    sets = cursor.fetchall()

    conn.close()

    return render_template("listsets.html", repo=repo, sets=sets)


@app.route("/identify/<int:repo_id>")
def getIdentify(repo_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM oai_identify WHERE id = %s", (repo_id,))
    data = cursor.fetchone()

    if data and not data.get("repository_name"):
        # agora usa a função update_identify
        oai_identify.update_identify(data)

        # recarrega os dados já atualizados
        cursor.execute("SELECT * FROM oai_identify WHERE id = %s", (repo_id,))
        data = cursor.fetchone()

    conn.close()
    return render_template("identify_detail.html", repo=data) + render_template("identify_actions.html", repo=data)


@app.route("/records")
def records():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM oai_records LIMIT 50")
    data = cursor.fetchall()
    conn.close()
    return render_template("records.html", records=data)

@app.route("/getrecords/<int:rec_id>")
def get_records(rec_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM oai_identify WHERE id = %s", (rec_id,))
    data = cursor.fetchone()
    url = data.get("base_url")
    print("=============",url)
    oai_listRecords.coletar_registros_oai(url)
    #return render_template("record.html", record=data)
    return "X"



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
