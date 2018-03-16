from himalaya_models.backends.hbase import HBaseBase


class HBaseMock(HBaseBase):
    hbase_table = 'table'
    hbase_fields = ('field',)
    hbase_family = 'family'
    hbase_row_key = 'row_key'

    # This behavior is done in by elasticsearch-dsl
    # that's why we don't need it on the models.py
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


hbase = HBaseMock(
    field='field',
    row_key='row_key',
)


def test_hbase_connection(mocker):
    mocker.patch(
        'happybase.Connection',
    )

    conn = hbase.connect_hbase()
    assert conn


def test_hbase_get_row_key(mocker):
    mocker.patch(
        'happybase.Connection',
    )
    conn = hbase.connect_hbase()
    conn.row = mocker.Mock(return_value={
        b'table:field': b'value',
    })

    obj = hbase.get_from_hbase('hash')
    assert obj


def test_hbase_save(mocker):
    mocker.patch(
        'happybase.Connection',
    )
    conn = hbase.connect_hbase()
    conn.put = mocker.Mock(return_value=True)

    saved = hbase.save_to_hbase()
    assert saved


def test_hbase_delete(mocker):
    mocker.patch(
        'happybase.Connection',
    )
    conn = hbase.connect_hbase()
    conn.delete = mocker.Mock(return_value=True)

    deleted = hbase.delete_from_hbase()
    assert deleted is None


def test_hbase_validate_fields():
    assert hbase.validate_fields()


def test_hbase_construct_save_payload():
    save_payload = hbase.construct_save_payload()
    assert save_payload == {b'family:field': b'field'}


def test_hbase_to_bytes():
    assert hbase.to_bytes('test') == b'test'


def test_hbase_dict_to_class():
    _dict = {b'family:field': b'field'}
    assert HBaseMock.dict_to_class(HBaseMock, _dict)
