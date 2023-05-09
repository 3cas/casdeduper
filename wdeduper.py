from datetime import datetime
import hashlib
import json
import sys
import os



if len(sys.argv) > 1:
    SCAN_DIR = sys.argv[1]
else:
    SCAN_DIR = os.path.expanduser("~")

hashes = {}

for root, dirs, files in os.walk(SCAN_DIR):
    for name in files:
        path = os.path.join(root, name)
        try:
            with open(path, "rb") as f:
                content = f.read()

        except:
            print(f"Skipping {path}...")

        else:
            md5 = hashlib.md5(content).hexdigest()
            
            if md5 in hashes:
                hashes[md5].append(path)
            else:
                hashes[md5] = [path]

dupes = {}

for md5 in hashes:
    if len(hashes[md5]) > 1:
        dupes[md5] = hashes[md5]

time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

with open(f"{time}_results.json", "w") as f:
    json.dump(dupes, f, indent=4)

print(f"Found {len(dupes)} duplicate hashes.")
ch = input("Would you like to move/delete the duplicate files now?")

def move():
    for md5 in dupes:
        for path in dupes[md5][1:]:
            root, name = os.path.split(path)
            original_name = name

            suffix = 1
            while os.path.isfile(os.path.join(MOVE_DIR, name)):
                suffix += 1
                name = f"{original_name}_{suffix}"

            os.rename(path, os.path.join(MOVE_DIR, name))

    