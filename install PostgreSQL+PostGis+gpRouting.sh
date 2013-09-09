#!/bin/bash
#
# Install Postgres 9.1, PostGIS 2.0 and pgRouting on a clean Ubuntu 12.04 install (64 bit)
# updated to PostGIS 2.0.1
 
# basics
apt-get install python-software-properties
# To get GEOS 3.3.3
apt-add-repository ppa:sharpie/for-science 
 
# install the following pacakages
apt-get -y install postgresql-9.1 postgresql-server-dev-9.1 postgresql-contrib-9.1 gdal-bin binutils libgeos-c1 libgeos-dev libgdal1-dev libxml2 libxml2-dev libxml2-dev checkinstall libproj-dev libpq-dev build-essential
 
# some missing packages for some reason
apt-get update --fix-missing
 
# run again
apt-get -y install postgresql-9.1 postgresql-server-dev-9.1 postgresql-contrib-9.1 gdal-bin binutils libgeos-c1 libgeos-dev libgdal1-dev libxml2 libxml2-dev libxml2-dev checkinstall libproj-dev libpq-dev build-essential
 
 
# fetch, compile and install PostGIS
wget http://postgis.refractions.net/download/postgis-2.0.2.tar.gz
tar zxvf postgis-2.0.2.tar.gz && cd postgis-2.0.2
./configure --with-raster --with-topology
make
make install
 
## pgrouting - Add pgRouting launchpad repository
sudo add-apt-repository ppa:georepublic/pgrouting
sudo apt-get update
 
# Install pgRouting packages
sudo apt-get install gaul-devel \
postgresql-9.1-pgrouting \
postgresql-9.1-pgrouting-dd \
postgresql-9.1-pgrouting-tsp
 
# Install osm2pgrouting package
sudo apt-get install libboost-all-dev
sudo apt-get install expat
#I have downloaded the src code of osm2pgrouting to /home/ruilin/pgRouting:
cd /home/ruilin/pgRouting/osm2pgrouting-master
make clean
make
 
# Install workshop material (optional)
sudo apt-get install pgrouting-workshop

# create a table called 'routing'
sudo su postgres
createdb routing

# add PostGIS functions (version 2.x) to 'routing'
psql -d routing -c "CREATE EXTENSION postgis;"
 
# add pgRouting core functions to 'routing'
psql -d routing -f /usr/share/postlbs/routing_core.sql
psql -d routing -f /usr/share/postlbs/routing_core_wrappers.sql
psql -d routing -f /usr/share/postlbs/routing_topology.sql
psql -d routing -f /usr/share/postgresql/9.1/contrib/postgis-2.0/legacy.sql

su ruilin

# Try to added osm map to database:
gedit /etc/postgresql/9.1/main/pg_hba.conf

#"local   all         all                               trust"

sudo /etc/init.d/postgresql restart

cd pgRouting/osm2pgrouting-master
./osm2pgrouting -file /home/ruilin/pgRouting/Beijing_Map/beijing.osm -conf ./mapconfig.xml -dbname routing -user postgres -host localhost -clean

if you want to enable php to access postgreSQL:
sudo apt-get install php5-pgsql
sudo service apache2 restart

if you want to see the data in the database,
sudo apt-get install pgAdmin3
pgadmin3
use localhost, postgres, to set up a connection

Next step: setup the geoserver

apt-get install tomcat7
down load geoserver-2.3.3-war, unzip and then
cp geoserver.war /var/lib/tomcat7/webapps
/etc/init.d/tomcat7 restart

maybe you can see the familiar GeoServer login page at (gasping) http://localhost:8080/geoserver/.

Configure Geoserver:
The default username and password is admin and geoserver
http://docs.geoserver.org/stable/en/user/gettingstarted/postgis-quickstart/index.html

Map Preparation:
#Download the users specified region @ http://download.bbbike.org/osm/
#or
#You download the partitioned map from openstreetmap and need to merge the OSM map files to import them all into database

sudo apt-get install osmosis
#Example:
osmosis --rx map1.osm --sort --rx map2.osm --sort --rx map3.osm --sort --merge --merge --wx sanfrancisco.osm

osmosis --rx 1-1.osm --sort --rx 1-2.osm --sort --rx 1-3.osm --sort --rx 2-1.osm --sort --rx 2-2.osm --sort --rx 2-3.osm --sort --rx 3-1.osm --sort --merge --merge --merge --merge --merge --merge --wx beijing.osm


#import mapdata from another database exportion
sudo su postgres
createdb routing
su ruilin
psql -U postgres -d routing -f /home/ruilin/pgRouting/beijing.out -h localhost

install GeoExt and Openlayers
just copy the www in pgRouting folder in to /var/www
GeoExt, OpenLayers and Ext 3.x will be ready for use.
next we just need to edit:
in routing-final.html: 
edit the openlayer position (need to change the server IP to real Ip, cannot be localhost).
edit the bounds of the map

in php/pgrouting.php: edit the database configurations


#use osm2po:

http://www.bostongis.com/PrinterFriendly.aspx?content_name=pgrouting_osm2po_1

psql -U postgres -d beijing -q -f "hh_2po_4pgr.sql" -h localhost




