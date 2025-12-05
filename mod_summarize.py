import mysql
import database


def get_indicator(repo_id, indicator, query):
    """
    Busca valor cacheado em Summarize.
    Se não existir, calcula pelo SQL e grava no cache.
    """
    mysql_config = database.config()
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor(dictionary=True)

    # tenta buscar no cache
    cursor.execute(
        """
        SELECT d_valor
        FROM Summarize
        WHERE d_indicator = %s
        ORDER BY d_created DESC
        LIMIT 1
    """, (f"{indicator}_{repo_id}", ))
    row = cursor.fetchone()

    if row:
        valor = row["d_valor"]
    else:
        # calcula de verdade
        cursor.execute(query, (repo_id, ))
        valor = cursor.fetchone().get("total", 0)

        # grava cache
        cursor.execute(
            """
            INSERT INTO Summarize (d_indicator, d_valor)
            VALUES (%s, %s)
        """, (f"{indicator}_{repo_id}", valor))
        conn.commit()

    conn.close()
    return valor
