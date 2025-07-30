ram_limit_g = None
tables_folder = 'tables'

triplestores = sorted(['tentris-baseline', 'tentris-insdel', 'virtuoso', 'graphdb', 'fuseki', 'oxigraph']) #, 'qlever'])

triplestore_order = ['Tentris-ID', 'Oxigraph', 'GraphDB', 'Fuseki', 'Virtuoso'] # , 'QLever']


def pretty_dataset_label(dataset):
    import matplotlib.text as mpltx
    if type(dataset) is mpltx.Text:
        triplestore = dataset.get_text()
    if dataset in pretty_dataset_label.mapping.values():
        return dataset
    return pretty_dataset_label.mapping[str(dataset)]


pretty_dataset_label.mapping = {'dbpedia': "DBpedia",
                                'wikidata': 'Wikidata',
                                # 'swdf': 'DummySWDF',
                                # 'bsbm': 'BSBM',
                                }
datasets = list(pretty_dataset_label.mapping.keys())


def pretty_triplestore_label(triplestore):
    import matplotlib.text as mpltx
    if type(triplestore) is mpltx.Text:
        triplestore = triplestore.get_text()
    if triplestore in pretty_triplestore_label.mapping.values():
        return triplestore
    return pretty_triplestore_label.mapping[str(triplestore)]


pretty_triplestore_label.mapping = {'tentris-baseline': "Tentris-baseline",
                                    'tentris-insdel': 'Tentris-ID',
                                    'virtuoso': 'Virtuoso',
                                    'graphdb': 'GraphDB',
                                    'fuseki': 'Fuseki',
                                    'oxigraph': "Oxigraph",
                                    "qlever": "QLever"}
