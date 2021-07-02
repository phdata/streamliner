snowsql-cmd=snowsql -c connection -f

evolve-schema: evolve-schema.sql
	$(snowsql-cmd) evolve-schema.sql

first-run:
	$(MAKE) evolve-schema