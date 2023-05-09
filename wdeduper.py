from datetime import datetime
from pick import pick
import hashlib
import sqlite3
import os


UPDATE_FREQUENCY = 1000


MSG_WELCOME = """
           ____           __                     
 _      __/ __ \___  ____/ /_  ______  ___  _____
| | /| / / / / / _ \/ __  / / / / __ \/ _ \/ ___/
| |/ |/ / /_/ /  __/ /_/ / /_/ / /_/ /  __/ /    
|__/|__/_____/\___/\__,_/\__,_/ .___/\___/_/     
                             /_/              

Welcome to wDeduper! Please select an option below.   
"""


def main():
    os.chdir(os.path.dirname(__file__))

    for scan_path in ["wdeduper", "wdeduper/scans"]:
        scan_path = os.path.join(*scan_path.split("/"))
        if not os.path.exists(scan_path):
            os.mkdir(scan_path)

    options = ["scan files"]
    options.append(f"view past scans ({len(os.listdir(os.path.join('wdeduper', 'scans')))})")
    options.append("quit")

    option, index = pick(options, MSG_WELCOME, ">")

    print(MSG_WELCOME)
    print("...\n")

    if index == 0:
        do_scan()
    elif index == 1:
        return
    else:
        return


def do_scan():
    print("Please enter the path of the directory you want to scan (default is ~)")
    while True:
        scan_path = input("> path: ")
        if not scan_path:
            scan_path = os.path.expanduser("~")
        
        if os.path.isdir(scan_path):
            break
        else:
            print("That's not a directory! Please try again (or ctrl+c to quit).")

    input(f"Ready! Press enter to start scanning '{scan_path}'... (or ctrl+c to quit)\n")

    print("Creating database...")

    time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    db_file = "scan_"+time+".db"
    db_path = os.path.join("wdeduper", "scans", db_file)

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute("CREATE TABLE keeps (path TEXT NOT NULL, size INTEGER NOT NULL, time INTEGER NOT NULL)")
    cur.execute("CREATE TABLE dupes (path TEXT NOT NULL, size INTEGER NOT NULL, time INTEGER NOT NULL, hash TEXT NOT NULL, original_path TEXT NOT NULL, original_time INTEGER NOT NULL)")

    print("Scanning files...")

    count = 0
    skips = 0

    for root, dirs, files in os.walk(scan_path):
        for filename in files:
            count += 1
            filepath = os.path.join(root, filename)

            try:
                stats = os.stat(filepath)

            except:
                skips += 1
            
            else:
                cur.execute("INSERT INTO keeps (path, size, time) VALUES (?, ?, ?)", (filepath, stats.st_size, stats.st_mtime))

            if count % UPDATE_FREQUENCY == 0:
                con.commit()
                print(f"~ Scanned {count} files so far, including {skips} skips. Still scanning...")

    con.commit()
    con.close()
    print(f"Scan complete! Scanned {count} files total, including {skips} skips.")
    if skips: print("NOTE: Skips are caused by insufficient read permissions. These can usually be ignored since important system files should not be deduped.")

    find_dupes(db_path)


def find_dupes(db_path: str):
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    print("Now looking for duplicate file sizes, and cleaning database...")

    all_files = cur.execute("SELECT size, time FROM keeps").fetchall()

    dupes = 0
    nondupes = 0
    hash_fails = 0

    checked_sizes = []

    for size, time in all_files:
        if size in checked_sizes:
            continue
        
        else:
            checked_sizes.append(size)
            
            size_matches = cur.execute("SELECT path, time FROM keeps WHERE size = ? ORDER BY time ASC", (size,)).fetchall()
            if len(size_matches) > 1:
                print(f"~ Found {len(size_matches)} files of {size} bytes, now checking hashes")
                
                by_hash = {}

                for path, time in size_matches:
                    try:
                        with open(path, "rb") as f:
                            content = f.read()

                    except:
                        hash_fails += 1

                    else:
                        md5 = hashlib.md5(content).hexdigest()

                        if md5 in by_hash:
                            print(f"! Found duplicate of {by_hash[md5][0]} at {path} (time new {time} > old {by_hash[md5][1]} ?)")
                            cur.execute("INSERT INTO dupes (path, size, time, hash, original_path, original_time) VALUES (?, ?, ?, ?, ?, ?)", (path, size, time, md5, by_hash[md5][0], by_hash[md5][1]))
                            cur.execute("DELETE FROM keeps WHERE path = ?", (path,))
                            dupes += 1

                        else:
                            by_hash[md5] = [path, time]
                            
            else:
                nondupes += 1

    con.commit()


if __name__ == "__main__":
    main()
