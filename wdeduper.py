from datetime import datetime
from pick import pick
import hashlib
import sqlite3
import random
import json
import os


MSG_WELCOME = """
           ____           __                     
 _      __/ __ \___  ____/ /_  ______  ___  _____
| | /| / / / / / _ \/ __  / / / / __ \/ _ \/ ___/
| |/ |/ / /_/ /  __/ /_/ / /_/ / /_/ /  __/ /    
|__/|__/_____/\___/\__,_/\__,_/ .___/\___/_/     
                             /_/              

Welcome to wDeduper! Please select an option below.   
"""

SETTINGS_FILE = os.path.join("wdeduper", "settings.json")

DEFAULT_SETTINGS = {
    "update_frequency": 1000,
    "min_size": 1000000
}


def save_settings():
    global settings
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f)


def main():
    os.chdir(os.path.dirname(__file__))

    for scan_path in ["wdeduper", "wdeduper/scans", "wdeduper/moved", "wdeduper/lists"]:
        scan_path = os.path.join(*scan_path.split("/"))
        if not os.path.exists(scan_path):
            os.mkdir(scan_path)

    global settings
    if os.path.isfile(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r") as f:
            settings = json.load(f)
    else:
        settings = DEFAULT_SETTINGS
        save_settings()

    options = {
        "scan files": do_scan, 
        f"view past scans ({len(os.listdir(os.path.join('wdeduper', 'scans')))})": view_scans, 
        "settings": settings_menu, 
        "quit": close,
    }
    
    option, index = pick(list(options.keys()), MSG_WELCOME, ">")

    print(MSG_WELCOME)
    print("...\n")

    options[option]()


def do_scan():
    print("Please enter the path of the directory you want to scan (default is ~)")
    while True:
        scan_path = input(" > ")
        if not scan_path:
            scan_path = os.path.expanduser("~")
        
        if scan_path[:2] == "~/":
            scan_path = os.path.join(os.path.expanduser("~"), scan_path.replace("~/", ""))

        if os.path.isdir(scan_path):
            break
        else:
            print("That's not a directory! Please try again (or ctrl+c to quit).")

    input(f"Ready! Press enter to start scanning '{scan_path}' (or ctrl+c to quit)\n")

    print("Creating database...")

    scanned_at = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    db_file = "scan_"+scanned_at+".db"
    db_path = os.path.join("wdeduper", "scans", db_file)

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute("CREATE TABLE keeps (path TEXT NOT NULL, size INTEGER NOT NULL, time INTEGER NOT NULL)")
    cur.execute("CREATE TABLE dupes (path TEXT NOT NULL, size INTEGER NOT NULL, time INTEGER NOT NULL, hash TEXT NOT NULL, original_path TEXT NOT NULL, original_time INTEGER NOT NULL)")
    cur.execute("CREATE TABLE data (scan_path TEXT NOT NULL, scanned_at TEXT NOT NULL)")
    cur.execute("INSERT INTO data (scan_path, scanned_at) VALUES (?, ?)", (scan_path, scanned_at))

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
                if stats.st_size > settings["min_size"]:
                    cur.execute("INSERT INTO keeps (path, size, time) VALUES (?, ?, ?)", (filepath, stats.st_size, stats.st_mtime))
                else:
                    skips += 1

            if count % settings["update_frequency"] == 0:
                con.commit()
                print(f"~ Scanned {count} files so far, including {skips} skips. Still scanning...")

    con.commit()

    print(f"Scan complete! Scanned {count} files total, including {skips} skips.")
    if skips: print("NOTE: Skips are caused by insufficient read permissions OR files smaller than the specified minimum sizes. These can usually be ignored since important system files should not be deduped.")

    print("\nNow looking for duplicate file sizes, and cleaning database...")

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
                #print(f"~ Found {len(size_matches)} files of {size} bytes, now checking hashes")
                
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
    con.close()

    print(f"All done! Found {dupes} duplicate files (this number includes each duplicate, but not the originals).")
        
    if dupes > 0:
        input("Press enter for options.")
        take_action(db_path)

    else:
        print("Since there aren't any, not doing anything. Bye!")


def take_action(db_path):
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    scan_path, scanned_at = cur.execute("SELECT scan_path, scanned_at FROM data").fetchone()
    duplicates = cur.execute("SELECT path FROM dupes").fetchall()

    option, index = pick(["move them all into one folder", "export a list of all files", "DELETE them forever", "quit"], f"What would you like to do with these {len(duplicates)} duplicate files?", ">")

    if index == 0:
        move_to = os.path.join("wdeduper", "moved", "moved_"+scanned_at)
        input(f"Okay, this operation will move all {len(duplicates)} files into {move_to}. Press enter to continue or ctrl+c to quit.")
        os.mkdir(move_to)

        for path, in duplicates:
            head, tail = os.path.split(path)

            if os.path.isfile(os.path.join(move_to, tail)):
                tail += "_" + str(random.randint(100000, 999999))

            os.rename(path, os.path.join(move_to, tail))

        print(f"Done! {len(os.listdir(move_to))} files have been moved.")

    elif index == 1:
        listing_file = os.path.join("wdeduper", "lists", "list_"+scanned_at+".txt")
        with open(listing_file, "w") as f:
            f.write("\n".join([path for path, in duplicates]))

        print(f"Saved all file paths to a text document at {listing_file}.")

    elif index == 2:
        print(f"You shouldn't use this option unless you really want to. It is much better to move all the files first using the 'move' option than to use the delete option. If you really want to delete, type 'Yes, I want to delete {len(duplicates)} files.'")
        check = input(" > ")
        if check == f"Yes, I want to delete {len(duplicates)} files.":
            for path, in duplicates:
                os.remove(path)
        else:
            print("Then I won't do it!")

    elif index == 3:
        close()


def view_scans():
    scans = os.listdir(os.path.join("wdeduper", "scans"))
    if scans:
        scan, index = pick(scans, "Which scan do you want to view/delete/etc?", ">")
        take_action(os.path.join("wdeduper", "scans", scan))
    else:
        print("There are no past scans!")


def settings_menu():
    print("There is no settings menu yet.")


def close():
    print("Thank you for using wDeduper.")


if __name__ == "__main__":
    main()
