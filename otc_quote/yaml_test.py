import yaml

stream = open("otc_options.yaml", "r")
docs = yaml.load_all(stream)
dict = {}
for doc in docs:
    for k,v in doc.items():
        dict.update({k:v})

print(dict)

