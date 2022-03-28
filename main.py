import subprocess
import pandas as pd
import json

## 1. fetch json from LDES

context = r"C:\Users\teugelso\PycharmProjects\pythonProject\TraintheTrainer_05\src\utils\context.jsonld"

endpoint = f"actor-init-ldes-client --pollingInterval 5000 --mimeType application/ld+json --context " + context + r" --fromTime 2022-03-20T00:00:00.309Z --emitMemberOnce false --disablePolling true https://apidg.gent.be/opendata/adlib2eventstream/v1/dmg/objecten > C:\Users\teugelso\PycharmProjects\pythonProject\TraintheTrainer_05\src\data\dmg_obj.json"
filepath = r"C:\Users\teugelso\PycharmProjects\pythonProject\TraintheTrainer_05\src\data\dmg_obj.json"

subprocess.run(endpoint, shell=True, text=True)

## fetch_json(filepath)

## 2.parse result into dataframe

def generate_dataframe(filepath):
    with open(filepath, encoding="utf8") as p:
        res = p.read()
        result = res.splitlines()
        print("done with parsing data from DMG")
        return result

df_dmg = pd.DataFrame(generate_dataframe(filepath))
columns = ["URI", "title", "beschrijving", "objectnummer", "object_name"]

##define actions

def fetch_title(df_dmg, range, _json):
    try:
        title = _json["http://www.cidoc-crm.org/cidoc-crm/P102_has_title"]
        df_dmg.at[range, "title"] = title["@value"]
    except Exception:
        pass

def fetch_beschrijving(df_dmg, range, _json):
    try:
        beschrijving = _json["http://www.cidoc-crm.org/cidoc-crm/P3_has_note"]
        df_dmg.at[range, "beschrijving"] = beschrijving["@value"]
    except Exception:
        pass

def fetch_objectnummer(df_dmg, range, _json):
    """parse object number from json"""
    try:
        object_number = _json["Entiteit.identificator"]
        for x in object_number:
            try:
                df_dmg.at[range, "objectnummer"] = x["skos:notation"]["@value"]
            except Exception:
                pass

    except Exception:
        pass

def fetch_objectname(df_dmg, range, _json):
    try:
        object_names = []
        # object_names.append(json["Entiteit.classificatie"])
        try:
            for i in _json["Entiteit.classificatie"]:
                for a in i["Classificatie.toegekendType"]:
                    on = a["skos:prefLabel"]["@value"]
                    object_names.append(on)
                    df_dmg.at[range, "object_name"] = object_names
        except Exception:
            pass
    except Exception:
        pass


for i in range(0, len(columns)):
    df_dmg.insert(i, columns[i], "")

for i in range(0, len(df_dmg)): ##change to len(df_dmg to fetch all)
    x = df_dmg.loc[i]
    j = json.loads(x[0])

    uri = j["http://purl.org/dc/terms/isVersionOf"]["@id"]
    df_dmg.at[i, "URI"] = uri

    fetch_title(df_dmg, i, j)
    fetch_beschrijving(df_dmg, i, j)
    fetch_objectnummer(df_dmg, i, j)
    fetch_objectname(df_dmg, i, j)

print(df_dmg)