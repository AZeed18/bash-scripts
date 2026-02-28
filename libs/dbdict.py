"""# Python Database API Dictionary Interface

Supports a single table, a single primary key and only modules that support `format`-style query parameters (i.e., `%s`)

Tested with pymysql, mariadb and mysqlclient

>>> sales = Table(mariadb, "sales", "transation_id", False)
>>> sales.connect(mariadb.Connection(...))

## INSERT/UPDATE Examples

### Table

>>> sales["T121"] = {"quantity": 4, "discount": 0.5}

### Pivot

>>> sales["T121"]["quantity"] = 4
>>> sales["T121"]["quantity", "discount"] = (4, 0.5)
>>> sales["T121", "T122"]["quantity"] = (4, 10)
>>> sales["T121", "T122"]["quantity", "discount"] = ((4, 0.5),(10, None))

## SELECT Examples

### Table

>>> sales["T121"]                                           # -> Pivot
>>> sales["T121", "T122"]                                   # -> Pivot
>>> sales.search('product = "Apple" AND price > 100')       # -> Pivot

### Pivot

>>> sales["T121"]["quantity"]                               # -> Any
>>> sales["T121"]["quantity", "discount"]                   # -> tuple
>>> sales["T121", "T122"]["quantity"]                       # -> tuple[Any]
>>> sales["T121", "T122"]["quantity", "discount"]           # -> tuple[tuple[Any]]
"""

from typing import Any, Iterable
from threading import local

class Table:
	"""module: module object
	connection: any DB API Connection object
	name: table name in database
	pk: primary key column name
	insert_only (False): by default, assigning to rows does insert or update if key exists, set to True to only insert which would raise OperationalError if exists"""
	def __init__(self, module, name: str, pk: str, insert_only: bool = False):
		self.name = name
		self.pk = pk
		self.insert_only = insert_only
		self.local = local()

	def connect(self, connection):
		self.local.con = connection
		self.local.cur = connection.cursor()
		# self.local.cur.execute(f'SELECT * FROM {self.name} LIMIT 0')
		# cols_info = self.local.cur.description
		# self.columns = tuple(col_info[0] for col_info in cols_info)

	def __getitem__(self, keys: tuple[Any] | Any | dict[str, Any]):
		if not isinstance(keys, tuple):
			keys=(keys,)

		return Pivot(self, keys)
	
	def query(self, query: str, parameters: Iterable[Any] = tuple()):
		"Executes query, commits and returns results of sqlite3.Cursor.fetchall"
		self.local.cur.execute(query, parameters)
		reults = self.local.cur.fetchall()
		self.local.con.commit()
		return reults

	def search(self, query: str, no_where: bool = False):
		"Search using SQL, start the query from after WHERE, use no_where to start directly from after selection"
		self.local.cur.execute(f'SELECT {self.pk} FROM {self.name}{" WHERE" if not no_where else ""} {query}')
		rows = self.local.cur.fetchall()
		return Pivot(self, tuple(row[0] for row in rows))

	def __setitem__(self, key: Any, values: dict[str, Any]):
		if self[key] and not self.insert_only:
			self.local.cur.execute(f'UPDATE {self.name} SET {(''.join(f'{col}=%s,' for col in values.keys()))[:-1]} WHERE {self.pk}=%s', tuple(values.values())+(key,))
			self.local.con.commit()
		else:
			values[self.pk] = key
			self.local.cur.execute(f'INSERT INTO {self.name} ({(''.join(f'{col},' for col in values.keys()))[:-1]}) VALUES ({('%s,'*len(values))[:-1]})', tuple(values.values()))
			self.local.con.commit()

	def __delitem__(self, keys: tuple[Any] | Any):
		if not isinstance(keys, tuple):
			keys=(keys,)

		keys=((key,) for key in keys)

		self.local.cur.executemany(f'DELETE FROM {self.name} WHERE {self.pk}=%s', keys)
		self.local.con.commit()

	def __len__(self):
		self.local.cur.execute(f'SELECT COUNT(*) FROM {self.name}')
		return self.local.cur.fetchone()[0]

	def rollback(self):
		self.local.con.rollback()

class Pivot(Iterable):
	def __init__(self, table: Table, keys: tuple[Any]):
		self.table = table
		self.keys = keys

	def __iter__(self):
		return (Pivot(self.table, [key]) for key in self.keys)

	def to_dict(self, keep_key: bool = False) -> dict[Any, dict[str, Any]] | dict[str, Any]:
		"""Converts rows into a dictionary, its keys are primary key values and its values are column-value pairs

		`keep_key`: This prevents ignoring dictionary keys and returning only columns values if there is only one row"""
		rows = {}
		for key in self.keys:
			self.table.local.cur.execute(f'SELECT * FROM {self.table.name} WHERE {self.table.pk}=%s', [key])
			row = self.table.local.cur.fetchone()
			if row is not None:
				cols = (col_info[0] for col_info in self.table.local.cur.description)
				rows[key] = dict(zip(cols, row))
				del rows[key][self.table.pk]
			else:
				rows[key] = {}

		if not keep_key and len(rows) == 1:
			return rows.popitem()[1]
		else:
			return rows

	def __getitem__(self, cols: tuple[str] | str, keep_tuples: bool = False):
		if not isinstance(cols, tuple):
			cols=(cols,)

		values = []
		for key in self.keys:
			self.table.local.cur.execute(f'SELECT {(''.join(f'{col},' for col in cols))[:-1]} FROM {self.table.name} WHERE {self.table.pk}=%s', (key,))
			row_values = self.table.local.cur.fetchone()

			if not keep_tuples and len(cols) == 1:
				if row_values is None:
					raise self.table.module.OperationalError(f'row {key} does not exist')
				else:
					row_values = row_values[0]

			values.append(row_values)

		if len(values) == 1:
			return values[0]
		elif not values:
			return None
		else:
			return tuple(values)

	def __setitem__(self, cols: tuple[str] | str, values: tuple[tuple[Any]] | tuple[Any] | Any):
		if len(self.keys) == 1:
			values=(values,)
		if not isinstance(cols, tuple):
			values=((value,) for value in values)
			cols=(cols,)

		values_with_keys = (row_values+(key,) for (row_values, key) in zip(values,self.keys))

		self.table.local.cur.executemany(f'UPDATE {self.table.name} SET {(''.join(f'{col}=%s,' for col in cols))[:-1]} WHERE {self.table.pk}=%s', values_with_keys)
		self.table.local.con.commit()

	def __add__(self, col: str, value: Any):
		return self[col] + col

	def __contains__(self, key: Any):
		return key in self.keys
	
	def __len__(self):
		rows = self.__getitem__(self.table.pk, True)

		if isinstance(rows, list):
			return len((row for row in rows if row is None))
		elif rows is None:
			return 0
		else:
			return 1
	
	def rollback(self):
		self.local.con.rollback()
