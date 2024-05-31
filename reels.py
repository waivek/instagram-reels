from instaloader.instaloader import Instaloader
from instaloader.structures import Profile, load_structure_from_file, save_structure_to_file, load_structure
from instaloader.nodeiterator import resumable_iteration, FrozenNodeIterator
import json

from waivek import Code
from waivek import ic
from waivek import Timer
timer = Timer()
from waivek import rel2abs
import os.path

from datetime import datetime, timedelta
import jsonpickle
from waivek import write
import timeago
from waivek import Connection
from waivek.ic import is_function
from types import NoneType

def create_table():
    # CREATE TABLE IF NOT EXISTS posts (shortcode TEXT PRIMARY KEY, video_url TEXT, video_view_count INTEGER, caption TEXT NOT NULL, caption_hashtags TEXT NOT NULL, caption_mentions TEXT NOT NULL, comments INTEGER NOT NULL, `date` INTEGER NOT NULL, date_local INTEGER NOT NULL, date_utc INTEGER NOT NULL, is_pinned INTEGER NOT NULL, is_video INTEGER NOT NULL, likes INTEGER NOT NULL, location TEXT, mediacount INTEGER NOT NULL, mediaid INTEGER NOT NULL, owner_id TEXT NOT NULL, owner_profile TEXT NOT NULL, owner_username TEXT NOT NULL, pcaption TEXT NOT NULL, profile TEXT NOT NULL, tagged_users TEXT NOT NULL, typename TEXT NOT NULL, url TEXT NOT NULL, viewer_has_liked TEXT) STRICT;
    schema_path = rel2abs("instagram.sql")
    connection = Connection("data/instagram.db")
    cursor = connection.cursor()
    cursor.executescript(open(schema_path).read())
    connection.commit()

def convert_value(value):
    if type(value) == datetime:
        return value.timestamp()
    if type(value) == list:
        return json.dumps(value)
    return value

def convert_custom_values(value):
    if type(value) == Profile:
        return value.username
    return value

def insert_post_into_db(post):
    connection = Connection("data/instagram.db")
    cursor = connection.cursor()
    values = [ getattr(post, key) for key in ["shortcode", "video_url", "video_view_count", "caption", "caption_hashtags", "caption_mentions", "comments", "date", "date_local", "date_utc", "is_pinned", "is_video", "likes", "location", "mediacount", "mediaid", "owner_id", "owner_profile", "owner_username", "pcaption", "profile", "tagged_users", "typename", "url", "viewer_has_liked"]]
    values = [ convert_value(value) for value in values]
    values = [ convert_custom_values(value) for value in values]
    sql = "INSERT INTO posts VALUES (" + ", ".join("?" * len(values)) + ")"
    cursor.execute(sql, values)
    connection.commit()

def get_pickle_path():
    return rel2abs('reels.json')

def save_posts(posts):
    pickle = jsonpickle.encode(posts, indent=4)
    assert pickle
    output_path = get_pickle_path()
    with open(output_path, 'w') as f:
        f.write(pickle)

def load_posts_or_empty_list() -> list:
    if not os.path.exists(get_pickle_path()) or os.path.getsize(get_pickle_path()) == 0:
        return []
    input_path = get_pickle_path()
    with open(input_path, 'r') as f:
        pickle = f.read()
    posts = jsonpickle.decode(pickle)
    assert type(posts) == list
    return posts

def post_to_dict(post):
    keys = dir(post)
    D = {}
    for key in keys:
        if not key.startswith('_'):
            D[key] = getattr(post, key)
    return D

def download_handle(handle):
    def get_output_path(magic):
        filename = f"resume_info_{magic}.json"
        return rel2abs(filename)
    def save_function(frozen_node_iterator, path):
        json.dump(frozen_node_iterator._asdict(), open(path, 'w'), indent=4)
    def load_function(_, path):
        return FrozenNodeIterator(**json.load(open(path)))
    L = Instaloader()
    print("Instaloader context created")
    profile = Profile.from_username(L.context, handle)
    print("Profile created")
    iterator_json_path = rel2abs('iterator.json')
    posts_iterator = profile.get_posts()

    days = 10
    stop_date = datetime.now() - timedelta(days=days)
    table = load_posts_or_empty_list()
    with resumable_iteration(
            context=L.context,
            iterator=posts_iterator,
            load=load_function,
            save=save_function,
            format_path=get_output_path
            ) as (is_resuming, start_index):
        for post in posts_iterator:
            post.owner_profile.full_name
            date = post.date_utc
            shortcode = post.shortcode
            shortcode_in_table = any(p.shortcode == shortcode for p in table)
            if shortcode_in_table:
                continue
            table.append(post)
            message = f"Processing post {shortcode} {timeago.format(date)}"
            print(message)
            if date < stop_date:
                days_ago = (datetime.now() - date).days
                print(f"Stop Date Days Count: {days}")
                print(f"Stopping at {date} ({days_ago} days ago)")
                break
    save_posts(table)


def get_posts_table(handle):
    # Initialize Instaloader

    # Define the Instagram handle and time frame
    # https://www.instagram.com/dallasmavs/reels/?hl=en

    one_year_ago = datetime.now() - timedelta(days=365)

    L = Instaloader()
    print("Instaloader context created")
    profile = Profile.from_username(L.context, handle)
    print("Profile created")
    # Get the reels of the profile
    posts_iterator = profile.get_posts()
    count = 0
    table = []
    for post in posts_iterator:
        print(f"Processing post {count}")
        D = post_to_dict(post)
        insert_post_into_db(D)
        breakpoint()
        table.append(D)
        count += 1


    return table

def foo():

    output_filename = rel2abs('reels.json')
    if not os.path.exists(output_filename) or os.path.getsize(output_filename) == 0:
        print(f"Creating {output_filename}")
        handle = 'dallasmavs'
        table = get_posts_table(handle)

        pickled_table = jsonpickle.encode(table, indent=4)
        if not pickled_table:
            raise ValueError("Table is empty")
        with open(output_filename, 'w') as f:
            f.write(pickled_table)
        print(f"Table written to {output_filename}")

    else:
        with open(output_filename, 'r') as f:
            pickled_table = f.read()
        table = jsonpickle.decode(pickled_table)
        assert type(table) == list
        table_2 = []
        for row in table:
            row_2 = [ (k, v) for k, v in row.items() if not is_function(v)]
            ic(row_2)
        # keys = table[0].keys()


def set_to_schema_string_and_converter(type_set: set) -> tuple:
    if len(type_set) > 3 or len(type_set) == 0:
        raise ValueError(f"Invalid type set: {type_set}")
    type_to_text = {
            int: "INTEGER",
            str: "TEXT",
            bool: "INTEGER",
            float: "REAL",
            datetime: "INTEGER",
            list: "TEXT"
    }
    type_to_callable = {
            int: None,
            str: None,
            bool: None,
            float: None,
            datetime: lambda x: x.timestamp(),
            list: json.dumps
    }
    if type_set == { NoneType }:
        return "TEXT", None
    nullable = False
    if NoneType in type_set:
        type_set.remove(NoneType)
        nullable = True
    assert len(type_set) == 1
    type_ = type_set.pop()
    text = type_to_text[type_]
    if not nullable:
        text += " NOT NULL"
    callable_function = type_to_callable[type_]
    return text, callable_function

def sqlite_schema_table_to_sqlite_create_table_string(schema_table: list, table_name: str, pk: str) -> str:
    lines = []
    for row in schema_table:
        column = row['column']
        type_ = row['type']
        line = f"    {column} {type_}"
        if column == pk:
            line = line.replace("NOT NULL", "").rstrip() + " PRIMARY KEY"
        lines.append(line)
    # put PRIMARY KEY line at top
    lines.sort(key=lambda x: "PRIMARY KEY" in x, reverse=True)
    columns = ",\n".join(lines)
    sql = f"CREATE TABLE {table_name} (\n{columns}\n) STRICT;"
    return sql

def generate_schema():
    table = load_posts_or_empty_list()
    if not table:
        print("Table is empty")
        return
    schema_dict = {}
    for post in table:
        # print("Processing post")
        # D = { key: getattr(post, key) for key in dir(post) if not key.startswith('_')}
        # D = { key: getattr(post, key) for key in dir(post) if not key.startswith('_')}
        D = {}
        for key in dir(post):
            if key.startswith('_'):
                continue
            if key in ["is_sponsored" , "sponsor_users", "title", "accessibility_caption", "video_duration" ]:
                continue
            timer.start(f"getattr {key}")
            value = getattr(post, key)
            time_taken = timer.get(f"getattr {key}")
            if time_taken > 0.01:
                print(f"Time taken for {key}: {time_taken}")
            if key in [ "owner_profile"]:
                D[key] = value.username
                continue
            D[key] = value

        D = {k: v for k, v in D.items() if not is_function(v)}
        for key, value in D.items():
            if schema_dict.get(key) is None:
                schema_dict[key] = set()
            schema_dict[key].add(type(value))
    schema_table = []
    for key, value in schema_dict.items():
        length = len(value)
        schema_table.append({
            'key': key,
            'value': value,
            'length': length
        })
     # sort by length DESC
    schema_table = sorted(schema_table, key=lambda x: x['length'], reverse=True)
    # ic(schema_table)

    sqlite_schema = []
    for row in schema_table:
        key = row['key']
        value = row['value']
        length = row['length']
        if value == {Profile}:
            print(Code.LIGHTRED_EX + f"{key}: {value}")
            breakpoint()
            continue
        schema_string, converter = set_to_schema_string_and_converter(value)
        sqlite_schema.append({ "column": key, "type": schema_string, "converter": converter})
    ic(sqlite_schema)
    table_name = "posts"
    pk = "shortcode"
    sql = sqlite_schema_table_to_sqlite_create_table_string(sqlite_schema, table_name, pk)
    print(sql)

def json_pickle_file_to_sqlite_table():
    posts = load_posts_or_empty_list()
    for post in posts:
        insert_post_into_db(post)

def main():
    import os
    data_directory = rel2abs('data')
    os.makedirs(data_directory, exist_ok=True)
    create_table()
    handle = 'dallasmavs'
    json_pickle_file_to_sqlite_table()
    # generate_schema()
    # download_handle(handle)

if __name__ == "__main__":
    main()

