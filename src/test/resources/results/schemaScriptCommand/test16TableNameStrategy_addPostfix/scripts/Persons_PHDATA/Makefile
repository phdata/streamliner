



snowsql-cmd=snowsql -c connection -f

currTableStateFile=/Users/anishkumar/anishKumar/dev/github/streamliner/src/test/resources/results/schemaScriptCommand/test16TableNameStrategy_addPostfix/state-directory/Persons.yml
previousStateDirectory=/Users/anishkumar/anishKumar/dev/github/streamliner/src/test/resources/results/schemaScriptCommand/test16TableNameStrategy_addPostfix/previous-state-directory

copy-into: copy-into.sql
	$(snowsql-cmd) copy-into.sql

create-schema: create-schema.sql
	$(snowsql-cmd) create-schema.sql

create-snowpipe: create-snowpipe.sql
	$(snowsql-cmd) create-snowpipe.sql

create-stage: create-stage.sql
	$(snowsql-cmd) create-stage.sql

create-table: create-table.sql
	$(snowsql-cmd) create-table.sql

drop-schema: drop-schema.sql
	$(snowsql-cmd) drop-schema.sql

drop-snowpipe: drop-snowpipe.sql
	$(snowsql-cmd) drop-snowpipe.sql

drop-stage: drop-stage.sql
	$(snowsql-cmd) drop-stage.sql

drop-table: drop-table.sql
	$(snowsql-cmd) drop-table.sql

alter-table-evolve-schema: alter-table-evolve-schema.sql
	$(snowsql-cmd) alter-table-evolve-schema.sql

alter-snowpipe: alter-snowpipe.sql
	$(snowsql-cmd) alter-snowpipe.sql

incompatible-change: incompatible-change.sql
	$(snowsql-cmd) incompatible-change.sql

first-run:
	$(MAKE) create-schema
	$(MAKE) create-stage
	$(MAKE) create-table
	$(MAKE) copy-into
	$(MAKE) create-snowpipe
	mv $(currTableStateFile) $(previousStateDirectory)

drop:
	$(MAKE) drop-table
	$(MAKE) drop-snowpipe

evolve-schema:
