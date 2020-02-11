import logging
import sys
from typing import Dict, List, Tuple

import aerospike
from aerospike import exception as ex
from aerospike import predicates as p


def connect(config: Dict[str, List[Tuple[str, int]]]) -> aerospike.Client:
    try:
        client = aerospike.client(config).connect()
        return client
    except ex.AerospikeError as e:
        logging.error(f"Cannot connect to host {config['hosts']}: {e.msg, e.code}")
        sys.exit(1)


class CustomerLifetime:
    CONFIG = {
        'hosts': [('127.0.0.1', 3000)]
    }

    def __init__(self, namespace: str, set_: str):
        self._client = connect(self.CONFIG)
        self.namespace = namespace
        self.set_ = set_

    def create_int_index(self, bin_name: str, idx_name: str) -> None:
        try:
            self._client.index_integer_create(self.namespace, self.set_, bin_name, idx_name)
        except ex.AerospikeError as e:
            logging.error(f"Cannot create integer index for bin {bin_name}: {e.msg, e.code}")

    def close_connection(self) -> None:
        self._client.close()

    def add_customer(self, id_customer: int, phone_number: int, lifetime_value: int) -> None:
        key = (self.namespace, self.set_, id_customer)
        try:
            self._client.put(key, {'phone': phone_number, 'ltv': lifetime_value})
        except ex.AerospikeError as e:
            logging.error(f"Cannot add a customer {id_customer} "
                          f"with attributes {phone_number}, {lifetime_value}: {e.msg, e.code}")

    def get_ltv_by_id(self, id_customer: int) -> int:
        key = (self.namespace, self.set_, id_customer)
        try:
            (key, metadata, record) = self._client.get(key)
            ltv = record.get('ltv', None)
            return ltv
        except ex.AerospikeError as e:
            logging.error(f"Cannot fetch ltv for customer {id_customer}: {e.msg, e.code}")

    def get_ltv_by_phone(self, phone_number: int) -> int:
        try:
            query = self._client.query(self.namespace, self.set_)
            res = query.select('phone', 'ltv').where(p.equals('phone', phone_number)).results()
            if not res:
                logging.error(f"Cannot find ltv for customer's phone number {phone_number}")
            return res[0][-1]['ltv']
        except ex.AerospikeError as e:
            logging.error(f"Cannot fetch ltv using customer's phone number {phone_number}: {e.msg, e.code}")


if __name__ == '__main__':
    cl = CustomerLifetime(namespace='test', set_='test')
    cl.create_int_index(bin_name='phone', idx_name='idx_int_phone')

    for i in range(1, 50):
        cl.add_customer(i, i, i + 1)

    for i in range(1, 50):
        assert (i + 1 == cl.get_ltv_by_id(i)), f"No LTV by ID {i}"
        assert (i + 1 == cl.get_ltv_by_phone(i)), f"No LTV by phone {i}"

    cl.close_connection()
