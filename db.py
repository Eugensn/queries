'''Пример библиотеки для работы с sql'''
import sqlite3
import datetime
import time
import csv
import re

file_db_name = r'file.db'
csv_delimiter = ';'
csv_delimiter_replace = ','
variant_delimiter = '{alt_variant}'
attachment_delimiter = '{attachment}'


# region db

# def db_transaction_pattern():
#     con = None
#     try:
#         con = sqlite3.connect(file_db_name)
#         cur = con.cursor()
#         cur.execute()
#         con.commit()
#     except sqlite3.Error as e:
#         if con:
#             con.rollback()
#         print(e)
#         exit()
#     finally:
#         if con:
#             con.close()


def db_create_tables():  # инициализирует таблицы, если не существуют

    con = sqlite3.connect(file_db_name)
    with con:
        cur = con.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS bingo(
        name TEXT NOT NULL,        
        stage INT NOT NULL,
        reply INT NOT NULL,
        variant INT NOT NULL,
        content_text TEXT,
        attachment1 TEXT,
        attachment2 TEXT,
        PRIMARY KEY (name, stage, reply, variant));
        """)

        cur.execute("""CREATE TABLE IF NOT EXISTS feedback(
        owner_id INT NOT NULL,
        post_id INT NOT NULL,
        id INT NOT NULL,
        reply_to_comment INT,
        reply_to_user INT,
        from_id INT,
        date INT,
        text TEXT,
        feedback_incriment INT NOT NULL,
        parents_stack INT,
        internal_date TEXT,
        parent_text TEXT,
        PRIMARY KEY (owner_id, post_id, id));
        """)

        cur.execute("""CREATE TABLE IF NOT EXISTS botreplies(
        owner_id INT NOT NULL,        
        post_id INT NOT NULL,
        id INT NOT NULL,
        reply_to_comment INT NOT NULL,   
        to_id INT,
        date INT,
        text TEXT,
        record_id_incriment INT NOT NULL,
        feedback__feedback_incriment INT NOT NULL,
        thread_id INT NOT NULL,
        bingo__name TEXT NOT NULL,
        bingo__stage INT NOT NULL,
        bingo__reply INT,
        bingo__variant INT,  
        internal_date TEXT, 
        attachments TEXT,     
        PRIMARY KEY (owner_id, post_id, id, reply_to_comment));
        """)

        cur.execute("""CREATE TABLE IF NOT EXISTS threads(
        owner_id INT NOT NULL,        
        post_id INT NOT NULL,
        start_comment_id INT NOT NULL,
        user_id INT,
        thread_id INT NOT NULL,
        PRIMARY KEY (owner_id, post_id, start_comment_id, user_id));
        """)

        cur.execute("""CREATE TABLE IF NOT EXISTS allowed(
        id	INTEGER NOT NULL, name TEXT,
        PRIMARY KEY(id));
        """)

        cur.execute("""CREATE TABLE IF NOT EXISTS keywords(
	    keyword TEXT NOT NULL, bingo_name TEXT,
	    PRIMARY KEY(keyword));
        """)

    con.close()



def db_get_keywords():  # возвращает таблицу keywords
    con = sqlite3.connect(file_db_name)
    con.row_factory = sqlite3.Row
    result = None
    with con:
        cur = con.cursor()
        cur.execute(
            """SELECT * FROM keywords""")
        result = cur.fetchall()
    con.close()
    return result


# создает новую запсиь в therad по данным thread_data и возвращает ее thread_id
def db_therad_new(thread_data):
    thread_id = None
    con = sqlite3.connect(file_db_name)
    with con:
        try:
            cur = con.cursor()
            cur.execute(
                """SELECT (IFNULL(MAX(thread_id), 0)+1) FROM threads""")
            thread_id = cur.fetchone()[0]

            cur.execute(f"""INSERT INTO 
            threads(owner_id, post_id, start_comment_id, user_id, thread_id) 
            VALUES('{thread_data['owner_id']}',{thread_data['post_id']},{thread_data['start_comment_id']},{thread_data['user_id']},{thread_id});""")

        except sqlite3.Error as e:
            print(e)

    return thread_id


# из therad по данным thread_data  возвращает thread_id (None если не найден)
def db_tread_get(thread_data):
    thread_id = None
    con = sqlite3.connect(file_db_name)
    with con:
        try:
            cur = con.cursor()
            cur.execute(
                """SELECT 
                    IFNULL(thread_id, -1) 
                FROM threads 
                WHERE 
                    owner_id=:owner_id
                    AND post_id=:post_id
                    AND start_comment_id=:start_comment_id
                    AND user_id=:user_id""",
                {
                    'owner_id': thread_data['owner_id'],
                    'post_id': thread_data['post_id'],
                    'start_comment_id': thread_data['start_comment_id'],
                    'user_id': thread_data['user_id']
                })
            result = cur.fetchone()
            if result:
                thread_id = result[0]

        except sqlite3.Error as e:
            print(e)

    return thread_id


# в транзакции создает можество записей в feedback из переданного списка
def db_add_feedback(new_records):
    dt_now = datetime.datetime.now()
    # dt_unix = int(time.mktime(dt_now.timetuple()))

    con = None
    try:
        con = sqlite3.connect(file_db_name)
        cur = con.cursor()
        for new_feedback in new_records:
            cur.execute(f"""INSERT INTO feedback(
                owner_id,
                post_id,
                id,
                reply_to_comment,
                reply_to_user,
                from_id,
                date,
                text,
                parents_stack,
                feedback_incriment,
                internal_date,
                parent_text) 
                VALUES(
                    {new_feedback['owner_id']},
                    {new_feedback['post_id']},
                    {new_feedback['id']},
                    {new_feedback['reply_to_comment']},
                    {new_feedback['reply_to_user']},
                    {new_feedback['from_id']},
                    {new_feedback['date']},
                    '{new_feedback['text']}',
                    {new_feedback['parents_stack']},
                    (SELECT IFNULL(MAX(feedback_incriment), 0) + 1 FROM feedback),
                    '{str(dt_now)}',
                    '{new_feedback['parent_text']}');""")
        con.commit()

    except sqlite3.Error as e:
        if con:
            con.rollback()
        print(e)

    finally:
        if con:
            con.close()


def db_add_botreplies(**kwargs):  # сохраняет в botreplies комментарий
    owner_id = kwargs['mention']['owner_id']
    post_id = kwargs['mention']['post_id']
    id = kwargs['id']
    reply_to_comment = kwargs['mention']['id']
    to_id = kwargs['mention']['user_id']
    text = kwargs['text']
    feedback__feedback_incriment = kwargs['mention']['feedback_incriment']
    thread_id = kwargs['thread_id']
    bingo__name = kwargs['bingo_name']
    bingo__stage = kwargs['bingo_stage']
    attachments = kwargs['attachments']
    # bingo__reply = kwargs['']
    # bingo__variant = kwargs['']

    dt_now = datetime.datetime.now()
    dt_unix = int(time.mktime(dt_now.timetuple()))

    con = sqlite3.connect(file_db_name)
    with con:
        cur = con.cursor()
        cur.execute(f"""INSERT INTO botreplies(
            owner_id,
            post_id,
            id,
            reply_to_comment,
            to_id,
            date,
            text,
            record_id_incriment,
            feedback__feedback_incriment,
            thread_id,
            bingo__name,
            bingo__stage,
            internal_date,
            attachments) 
            VALUES(
                {owner_id},
                {post_id},
                {id},
                {reply_to_comment},
                {to_id},
                {dt_unix},
                '{text}',
                (SELECT IFNULL(MAX(record_id_incriment), 0) + 1 FROM botreplies),
                {feedback__feedback_incriment},
                {thread_id},
                '{bingo__name}',
                {bingo__stage},
                '{str(dt_now)}',
                '{attachments}'
                );""")


def db_get_allowed():  # возвращает идентификаторы разрешенных групп
    con = sqlite3.connect(file_db_name)

    result = None
    with con:
        cur = con.cursor()
        cur.execute(
            """SELECT * FROM allowed""")
        result = cur.fetchall()
    con.close()
    return result


def db_feedback_max_date():  # возвращает время последнего упомения из feedback
    con = sqlite3.connect(file_db_name)

    result = None
    with con:
        cur = con.cursor()
        cur.execute(
            """SELECT IFNULL(MAX(date), 0) FROM feedback""")
        result = cur.fetchone()
    con.close()
    return result[0]


def db_feedback_get_unprocessed():  # возвращает результат запроса с необработанными (т.е. неотвеченными по данным botreplies) строками в feedback
    con = sqlite3.connect(file_db_name)
    con.row_factory = sqlite3.Row

    with con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT 
                feedback.owner_id,
                feedback.post_id, 
                feedback.id,
                feedback.from_id as user_id,
                feedback.date,
                feedback.text,
                feedback.parents_stack as start_comment_id,
                feedback.feedback_incriment,
                feedback.parent_text,
                feedback.reply_to_comment

            FROM 
                feedback LEFT JOIN botreplies ON
                (feedback.owner_id = botreplies.owner_id 
                AND feedback.post_id = botreplies.post_id
                AND feedback.id = botreplies.reply_to_comment)

            WHERE 
                botreplies.feedback__feedback_incriment IS NULL;                                 
                        """)
        result = cur.fetchall()
    con.close()
    return result


def db_feedback_get_unprocessed_allowed():  # возвращает результат запроса с необработанными (т.е. неотвеченными по данным botreplies) строками в feedback тольо для разрешенных групп
    con = sqlite3.connect(file_db_name)
    con.row_factory = sqlite3.Row

    with con:
        cur = con.cursor()
        cur.execute(
            """
            SELECT 
                feedback.owner_id,
                feedback.post_id, 
                feedback.id,
                feedback.from_id as user_id,
                feedback.date,
                feedback.text,
                feedback.parents_stack as start_comment_id,
                feedback.feedback_incriment,
                feedback.parent_text,
                feedback.reply_to_comment

            FROM 
                feedback LEFT JOIN botreplies ON
                (feedback.owner_id = botreplies.owner_id 
                AND feedback.post_id = botreplies.post_id
                AND feedback.id = botreplies.reply_to_comment)
                LEFT JOIN allowed ON (feedback.owner_id = allowed.id)

            WHERE 
                botreplies.feedback__feedback_incriment IS NULL
                AND allowed.id IS NOT NULL;                                 
                        """)
        result = cur.fetchall()
    con.close()
    return result


def db_get_botreply(owner_id, post_id, id): #возвращает ответ бота по заданным параметрам
    con = sqlite3.connect(file_db_name)
    con.row_factory = sqlite3.Row
    result = None
    with con:
        cur = con.cursor()
        cur.execute(
            """SELECT * from botreplies WHERE owner_id=:owner_id AND post_id=:post_id AND id=:id""", {'owner_id': owner_id, 'post_id':post_id, 'id':id})
        result = cur.fetchone()
    con.close()
    return result if result == None else result


def db_read_bingo_roworder(name): # возвращает результат заспроса к bingo по указанному имени name с сортировкой по reply, stage, variant
    con = sqlite3.connect(file_db_name)
    con.row_factory = sqlite3.Row
    result = None
    with con:
        cur = con.cursor()
        cur.execute(
            """SELECT * from bingo WHERE name=:name ORDER BY reply, stage, variant""", {'name': name})
        result = cur.fetchall()
    con.close()
    return result


# возвращает результат заспроса к bingo по указанному имени name с сортировкой по stage, reply
def db_read_bingo_stage(name, stage):
    con = sqlite3.connect(file_db_name)
    con.row_factory = sqlite3.Row
    result = None
    with con:
        cur = con.cursor()
        cur.execute(
            """SELECT * from bingo WHERE name=:name AND stage=:stage ORDER BY reply""", {'name': name, 'stage': stage})
        result = cur.fetchall()
    con.close()
    return get_bingo(result)


def get_bingo(result):  # преобразует переданный результат заспроса db_read_bingo_stage в список reply
    # bingo_stages = []
    # prev_stage = None
    stage_row = []
    prev_reply = -1
    for record in result:

        col_increase = (record['reply'] - prev_reply)
        if col_increase == 0:  # добавить в текущую "колонку reply" вариант
            stage_row[len(stage_row) -
                      1].append({'content_text': record['content_text'],
                                 'attachment1': record['attachment1'],
                                 'attachment2': record['attachment2']})

        else:
            # пока не будем пропущенные ответы заполнять пустыми строками
            # while col_increase > 1:  # пропущены колонки reply
            #     prev_reply += 1
            #     col_increase = (record['reply'] - prev_reply)
            #     stage_row.append([].append(''))
            # prev_reply += 1
            prev_reply = record['reply']

            variants = list()
            variants.append({'content_text': record['content_text'],
                            'attachment1': record['attachment1'],
                             'attachment2': record['attachment2']})
            stage_row.append(variants)

    return stage_row


# возвращает минимальный stage для bingo указанного в name
def db_get_min_stage(name):
    con = sqlite3.connect(file_db_name)
    with con:
        cur = con.cursor()
        cur.execute(
            """SELECT min(stage) from bingo WHERE name=:name""", {'name': name})
        result = cur.fetchone()
    con.close()
    return result[0]

# возвращает ответы для бинго с именем name и уровнем stage


def db_get_bingo_reply(name: str, stage: int):
    con = sqlite3.connect(file_db_name)
    result = None
    with con:
        cur = con.cursor()
        cur.execute(
            """SELECT IFNULL(content_text, '') from bingo WHERE name=:name AND stage=:stage""", {'name': name, 'stage': stage})
        result = cur.fetchall()
    con.close()
    return result


# добавляет в bingo новое бинго с указанным name и данными  в rows
def db_add_new_bingo(name: str, rows: list):

    con = sqlite3.connect(file_db_name)
    with con:
        try:
            cur = con.cursor()
            for row in rows:
                cur.execute(f"""INSERT INTO 
                bingo(name, stage, reply, variant, content_text, attachment1, attachment2) 
                VALUES('{name}',{row['stage']},{row['reply']},{row['variant']},'{row['content_text']}', '{row['attachment1']}', '{row['attachment2']}');""")

        except sqlite3.Error as e:
            print(e)


def db_dell_bingo(name: str):  # удаляет из bingo бинго с именем name
    con = sqlite3.connect(file_db_name)
    with con:
        try:
            cur = con.cursor()
            cur.execute(f"""DELETE FROM bingo WHERE name = '{name}';""")
        except sqlite3.Error as e:
            print(e)

    con.close()


# возвращает список имен бинго, доступных для переданного thread_id. если all=True, то всех, иначе только не использованных в botreplies
# с переданным thread_id
def db_get_bingo_names(thread_id, all=False):
    con = sqlite3.connect(file_db_name)
    with con:
        cur = con.cursor()
        cur.execute(
            f"""SELECT
                        bingo.name
                    FROM
                        bingo LEFT JOIN botreplies ON bingo.name = botreplies.bingo__name AND botreplies.thread_id = {thread_id}
                    WHERE botreplies.bingo__name IS NULL OR {all}
                    GROUP BY bingo.name""")
        result = cur.fetchall()
    con.close()
    return result


# возвращает по переданному thread_id данные об используемом бинго: name и следующий stage
def db_get_current_bingoinfo(thread_id):
    con = sqlite3.connect(file_db_name)
    result = None
    with con:
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT
                bingo.name,
                min(bingo.stage) As nextstage
                
            FROM bingo
            LEFT JOIN (SELECT 
                            bingo__name, 
                            bingo__stage, 
                            max(date) AS maxdate 
                        FROM botreplies 
                        WHERE botreplies.thread_id = {str(thread_id)}) AS br 
                        ON bingo.name = br.bingo__name 
                        AND bingo.stage > br.bingo__stage 
                        WHERE NOT br.bingo__name IS NULL             
            """)
        result = cur.fetchone()
    con.close()
    return result


# region service


def db_clear_table(table_name: str):  # очищает в БД таблицу с именем table_name
    con = sqlite3.connect(file_db_name)
    with con:
        try:
            cur = con.cursor()
            cur.execute(f"""TRUNCATE TABLE {table_name};""")
        except sqlite3.Error as e:
            print(e)

    con.close()


def db_drop_table(table_name: str):  # удаляет из БД таблицу с именем table_name
    con = sqlite3.connect(file_db_name)
    with con:
        try:
            cur = con.cursor()
            cur.execute(f"""DROP TABLE {table_name};""")
        except sqlite3.Error as e:
            print(e)

    con.close()

# endregion

# endregion


# region cvs

# преобразует результат заспроса bingo в бинго-таблицу, возвращая ее строки (для дальнейшего сохранения в файл)
def get_bingo_cvs(result, replace_cvs_delimeter=True):
    bingo_rows = []
    prev_stage = -1
    row = []
    prev_reply = None
    for record in result:

        if record['reply'] != prev_reply:  # новая "строка"
            prev_stage = -1
            row = []
            bingo_rows.append(row)

        col_increase = (record['stage'] - prev_stage)
        if col_increase == 0:  # добавить в текущую колонку
            variant = record['content_text']
            if replace_cvs_delimeter:
                variant.replace(
                    csv_delimiter, csv_delimiter_replace)

            if record['attachment1'] != None:
                variant = "".join(
                    [variant, ' ', attachment_delimiter, record['attachment1']])
            if record['attachment2'] != None:
                variant = "".join(
                    [variant, ' ', attachment_delimiter, record['attachment2']])

            row[len(row)-1] = "".join([row[len(row)-1],
                                       '\n', variant_delimiter, variant])
        else:  # пропущены колонки
            while col_increase > 1:
                prev_stage += 1
                col_increase = (record['stage'] - prev_stage)
                row.append('')
            prev_stage += 1
            prev_reply = record['reply']

            content_text = record['content_text']
            if replace_cvs_delimeter:
                content_text = content_text.replace(
                    csv_delimiter, csv_delimiter_replace)

            if record['attachment1'] != None:
                content_text = "".join(
                    [content_text, ' ', attachment_delimiter, record['attachment1']])
            if record['attachment2'] != None:
                content_text = "".join(
                    [content_text, ' ', attachment_delimiter, record['attachment2']])

            row.append(content_text)

    return bingo_rows


# сохраняет в файл данные bingo (в формате бинго)
def csv_save_bingo(bingo: list, bingo_filename_csv: str):
    with open(bingo_filename_csv, mode='w', newline='') as f:
        f_writer = csv.writer(f, delimiter=csv_delimiter,
                              quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in bingo:
            f_writer.writerow(row)

        f.close


# читает из файла бинго и возврачает список строк в формате БД
def csv_read_bingo(bingo_filename_csv):
    with open(bingo_filename_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=csv_delimiter)
        rows = []
        reply_count = 0
        for row in csv_reader:
            stage_count = -1

            for record in row:
                stage_count += 1
                variants = record.split(variant_delimiter)
                variant_count = -1
                for variant in variants:
                    content = variant.split(attachment_delimiter)
                    content_text = content[0]

                    attachment1 = ''
                    attachment2 = ''
                    if len(content) > 1:
                        attachment1 = content[1]
                    if len(content) > 2:
                        attachment2 = content[2]

                    variant_count += 1
                    rows.append({'stage': stage_count,
                                 'reply': reply_count,
                                 'variant': variant_count,
                                 'content_text': content_text,
                                 'attachment1': attachment1,
                                 'attachment2': attachment2})
            reply_count += 1

        return rows


