POSTGRESQL_VERSION ?= $(shell pg_config --version | awk '{print $$2}' | cut -d.  -f1)
POSTGIS_SCRIPT_DIR ?= $(shell find `pg_config --sharedir`/contrib/ -name 'postgis-*' -type d | sort -n | head -1)

check: loader 00-regress
	export PGPORT=`pg_lsclusters $(POSTGRESQL_VERSION) -j | jq -r '.[0].port'`; \
  echo "PGPORT: $$PGPORT"; \
	POSTGIS_REGRESS_DB=nibio_reg ./run_test.pl --spatial_ref_sys execute_parallel

debug:
	@echo POSTGIS_SCRIPT_DIR=$(POSTGIS_SCRIPT_DIR)

loader:
	mkdir -p ../loader; \
	ln -fs /bin/true ../loader/shp2pgsql; \
	ln -fs /bin/true ../loader/pgsql2shp

00-regress:
	@test -f $(POSTGIS_SCRIPT_DIR)/postgis.sql || \
    { echo "No postgis.sql file found in $(POSTGIS_SCRIPT_DIR)," \
           "please tweak \$$POSTGIS_SCRIPT_DIR" && exit 1; }
	@test -f $(POSTGIS_SCRIPT_DIR)/topology.sql || \
    { echo "No topology.sql file found in $(POSTGIS_SCRIPT_DIR)," \
           "please tweak \$$POSTGIS_SCRIPT_DIR" && exit 1; }
	test -e 00-regress-install/share/contrib/postgis || { \
	mkdir -p 00-regress-install/share/contrib; \
	ln -fs $(POSTGIS_SCRIPT_DIR) 00-regress-install/share/contrib/postgis; \
  }
