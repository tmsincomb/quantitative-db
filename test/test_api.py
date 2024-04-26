import json
import pprint
from quantdb.api import make_app
from flask_sqlalchemy import SQLAlchemy


def test():
    db = SQLAlchemy()
    app = make_app(db=db)
    client = app.test_client()
    runner = app.test_cli_runner()

    dataset_uuid = 'aa43eda8-b29a-4c25-9840-ecbd57598afc'
    some_object = '414886a9-9ec7-447e-b4d8-3ae42fda93b7'  # XXX FAKE
    actual_package_uuid = '15bcbcd5-b054-40ef-9b5c-6a260d441621'
    base = 'http://localhost:8989/api/1/'
    urls = (
        f'{base}values/inst?dataset={dataset_uuid}',
        f'{base}values/inst?dataset={dataset_uuid}&aspect=distance&aspect=time',
        f'{base}values/inst?dataset={dataset_uuid}&aspect=distance&value-quant-min=0.5',


        f'{base}objects?dataset={dataset_uuid}',
        f'{base}objects?dataset={dataset_uuid}&aspect=distance',
        f'{base}objects?dataset={dataset_uuid}&aspect=distance&value-quant-min=0.5',  # expect nothing
        f'{base}objects?dataset={dataset_uuid}&aspect=distance&value-quant-min=0.5&union-cat-quant=true',

        f'{base}values/quant?dataset={dataset_uuid}&aspect=distance',
        f'{base}values/quant?object={actual_package_uuid}&aspect=distance',

        f'{base}values/cat?object={actual_package_uuid}',
        f'{base}values/cat?object={actual_package_uuid}&union-cat-quant=true',  # shouldn't need it in this case

        f'{base}values/cat-quant?object={actual_package_uuid}',
        f'{base}values/cat-quant?object={actual_package_uuid}&union-cat-quant=true',

        f'{base}values?dataset={dataset_uuid}&aspect=distance&value-quant-min=0.5',
        f'{base}values?dataset={dataset_uuid}&aspect=distance&value-quant-min=0.5&union-cat-quant=true',

        f'{base}values?object={actual_package_uuid}',
        f'{base}values?object={actual_package_uuid}&union-cat-quant=true',

        f'{base}values/inst?object={actual_package_uuid}',
        f'{base}values/inst?object={actual_package_uuid}&union-cat-quant=true',

        f'{base}desc/inst',
        f'{base}desc/cat',
        f'{base}desc/quant',

        f'{base}terms',
        f'{base}aspects',
        f'{base}units',
        # TODO maybe shapes here as well?
    )
    resps = []
    for url in urls:
        resp = client.get(url)
        resp.ok = resp.status_code < 400
        resp.url = resp.request.url
        resp.content = resp.data
        resps.append(json.loads(resp.data.decode()))

    pprint.pprint(resps, width=120)
    breakpoint()

