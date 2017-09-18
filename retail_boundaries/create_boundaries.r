source("~/Dropbox/git/locoh/locoh.r")
source("~/Dropbox/git/postgis/postgis_functions.r")

con <- dbConnect(PostgreSQL(), dbname = "all_boundaries", user = "user", password = "pwd")

working_dir <- "~/Dropbox/liverpool/catchments/catchments2/retail_units"
setwd(working_dir)

import_or_append(con = con, working_dirs = getwd(), shp_names = "retail_units_all_data_area_id_names3.shp", srid = 27700, table_name = "clusters")

# a) Segmentize the building points to at least 20m and dump them (there will be max 1 point for every 10m)
dbExecute(con, "CREATE TABLE buildings_points2 (gid serial PRIMARY KEY, building_id int, geom geometry(Point,27700))")
dbExecute(con, paste("INSERT INTO buildings_points2(building_id, geom)",
                     "SELECT gid, (ST_DumpPoints((ST_Dump(ST_Segmentize(ST_RemoveRepeatedPoints(geom, 10), 20))).geom)).geom geom FROM buildings"))
dbExecute(con, "CREATE INDEX ON buildings_points2 USING gist(geom)")

# b) Select nearest building points (80m)
dbExecute(con, "CREATE TABLE buildings_points_clusters(gid serial PRIMARY KEY, cluster_id text, cl_id2 int, building_id int, geom geometry(Point,27700))")
dbExecute(con, paste("INSERT INTO buildings_points_clusters(cluster_id, cl_id2, building_id, geom)",
                     "SELECT t1.cluster_id, t1.cl_id2, t1.building_id, ST_RemoveRepeatedPoints(t2.geom, 10) FROM",
                     "(SELECT DISTINCT subq.cluster_id, subq.cl_id2, bp.building_id, bp.geom FROM buildings_points2 bp",
                     "INNER JOIN",
                     "(SELECT cl.cluster_id cluster_id, cl.cl_id2,",
                     "(SELECT bp_b.gid",
                     "FROM buildings_points2 bp_b",
                     "ORDER BY bp_b.geom <-> cl.geom LIMIT 1) point_id",
                     "FROM clusters cl) subq",
                     "ON bp.gid = subq.point_id",
                     "GROUP BY subq.cluster_id, subq.cl_id2, bp.building_id, bp.geom) t1, ",
                     "buildings_points2 t2 WHERE ST_DWithin(t1.geom, t2.geom, 80) AND t1.building_id = t2.building_id"
                     ))
dbExecute(con, "CREATE INDEX ON buildings_points_clusters(cl_id2)")
dbExecute(con, "CREATE INDEX ON buildings_points_clusters USING gist(geom)")

# b2) For retail parks select the nearest building
dbExecute(con, "CREATE TABLE buildings_points_retail_parks(gid serial PRIMARY KEY, cluster_id text, cl_id2 int, building_id int, geom geometry(Point,27700))")
dbExecute(con, paste("INSERT INTO buildings_points_retail_parks(cluster_id, cl_id2, building_id, geom)",
                     "SELECT t1.cluster_id, t1.cl_id2, t1.building_id, ST_RemoveRepeatedPoints(t2.geom, 5) FROM",
                     "(SELECT DISTINCT subq.cluster_id, subq.cl_id2, bp.building_id, bp.geom FROM buildings_points2 bp",
                     "INNER JOIN",
                     "(SELECT cl.cluster_id cluster_id, cl.cl_id2,",
                     "(SELECT bp_b.gid",
                     "FROM buildings_points2 bp_b",
                     "ORDER BY bp_b.geom <-> cl.geom LIMIT 1) point_id",
                     "FROM clusters cl WHERE cl.cluster_id LIKE 'RC%') subq",
                     "ON bp.gid = subq.point_id",
                     "GROUP BY subq.cluster_id, subq.cl_id2, bp.building_id, bp.geom) t1, ",
                     "buildings_points2 t2 WHERE t1.building_id = t2.building_id"
))
dbExecute(con, "CREATE INDEX ON buildings_points_retail_parks(cl_id2)")
dbExecute(con, "CREATE INDEX ON buildings_points_retail_parks USING gist(geom)")


############################################# LOCOH for town centres #################################################################

clusters_table = "buildings_points_clusters"; geom_field = "geom"; clusters_field = 'cl_id2';  points_id_field = "gid"

clusters3 <- st_read_db(con, clusters_table)

sort(table(clusters3$cl_id2), decreasing = T)[1:5]
#  2956  3091   671   608   624 
# 65759 26851 14586 13398 12817 
# do locoh for cl_id2 -> 2956, 3091

create_postgis_locoh_func(con = con, clusters_table = clusters_table, geom_field = geom_field, clusters_field = clusters_field, points_id_field = points_id_field)

locoh1 <- postgis_locoh(con = con,
                        clusters_table = clusters_table,
                        clusters_field = clusters_field,
                        cluster_id = 2956,
                        k_max = 20)
locoh_sfc <- st_as_sfc(st_as_text(locoh1), crs = 27700)
locoh_sf <- data.frame(cl_id2 = 2956)
locoh_sf$geometry <- locoh_sfc
locoh_sf <- st_as_sf(locoh_sf)

setwd("~/Dropbox/liverpool/boundaries/new_boundaries")
st_write(locoh_sf, paste0(getwd(), "/locoh1.shp"), driver = "ESRI Shapefile")

locoh2 <- postgis_locoh(con = con,
                        clusters_table = clusters_table,
                        clusters_field = clusters_field,
                        cluster_id = 3091,
                        k_max = 20)

rest_tc_ids <- clusters3[substr(clusters3$cluster_id,1,2) != "RC" & ! clusters3$cl_id2 %in% c(2956, 3091), ]$cl_id

cuts <- cut(rest_tc_ids,10,right = T)

cl_ids3 <- unique(rest_tc_ids[cuts == levels(cuts)[1]])
locoh3 <- lapply(cl_ids3, function(cluster_id) postgis_locoh(con = con,
                                                             clusters_table = clusters_table,
                                                             clusters_field = clusters_field,
                                                             cluster_id = cluster_id,
                                                             k_max = 20))

locoh_sfc <- st_as_sfc(do.call(rbind, lapply(locoh12, function(x) st_as_text(x))), crs = 27700)
locoh_sf <- data.frame(cl_id2 = cl_ids12)
locoh_sf$geometry <- locoh_sfc
locoh_sf <- st_as_sf(locoh_sf)
st_write(locoh_sf, paste0(getwd(),"/locoh_12.shp"), driver = "ESRI Shapefile")

drop_postgis_locoh_func(con, clusters_table = clusters_table)

############################################# LOCOH for retail parks #################################################################

clusters_table = "buildings_points_retail_parks"; geom_field = "geom"; clusters_field = 'cl_id2';  points_id_field = "gid"

create_postgis_locoh_func(con = con, clusters_table = clusters_table, geom_field = geom_field, clusters_field = clusters_field, points_id_field = points_id_field)


clusters4 <- st_read_db(con, clusters_table)
rp_ids <- unique(clusters4$cl_id2)

locoh_rps <- lapply(rp_ids, function(cluster_id) postgis_locoh(con = con,
                                                              clusters_table = clusters_table,
                                                              clusters_field = clusters_field,
                                                              cluster_id = cluster_id,
                                                              k_max = 25))

locoh_sfc <- st_as_sfc(do.call(rbind, lapply(locoh_rps, function(x) st_as_text(x))), crs = 27700)
locoh_sf <- data.frame(cl_id2 = rp_ids)
locoh_sf$geometry <- locoh_sfc
locoh_sf <- st_as_sf(locoh_sf)
st_write(locoh_sf, paste0(getwd(),"/locoh_rps.shp"), driver = "ESRI Shapefile")

drop_postgis_locoh_func(con, clusters_table = clusters_table)

##### 3. Import the boundaries into PostGIS ####################################################################################################
working_dir <- "~/Dropbox/liverpool/boundaries/new_boundaries"
shp_names <- get_shp_names(working_dir = working_dir, pattern = "locoh")
shp_names <- shp_names[! shp_names %in% c("locoh1.shp", "locoh2.shp", "locoh_rps.shp")]

import_or_append(con = con, working_dirs = working_dir, shp_names = shp_names, srid = 27700, table_name = "locoh_tc")

import_or_append(con = con, working_dirs = working_dir, shp_names = shp_names[4], srid = 27700, table_name = "locoh_london")
import_or_append(con = con, working_dirs = working_dir, shp_names = shp_names[5], srid = 27700, table_name = "locoh_london_west")

import_or_append(con = con, working_dirs = working_dir, shp_names = "locoh_rps.shp", srid = 27700, table_name = "locoh_rp")

dbExecute(con, "create table locoh_london2(cl_id2 text, geom geometry(MultiPolygon, 27700));")
dbExecute(con, "insert into locoh_london2 select cl_id2, st_collectionextract(st_makevalid(geom),3) from locoh_london;")

dbExecute(con, "create table locoh_london_west2(cl_id2 text, geom geometry(MultiPolygon, 27700));")
dbExecute(con, "insert into locoh_london_west2 select cl_id2, st_collectionextract(st_makevalid(geom),3) from locoh_london_west;")

dbExecute(con, "create table locoh_rp2(cl_id2 integer, geom geometry(MultiPolygon, 27700));")
dbExecute(con, "insert into locoh_rp2 select cl_id2, st_collectionextract(st_makevalid(geom),3) from locoh_rp;")

##### 4. Buffer out 20m in 18m, remove holes ###################################################################################################

dbExecute(con, "CREATE TABLE boundaries_except_london(cluster_id text, cl_name text, geom geometry (polygon, 27700))")
dbExecute(con, paste("INSERT INTO boundaries_except_london(cluster_id, cl_name, geom)",
                     "SELECT t2.cluster_id, t2.cl_name, t1.geom FROM",
                     "(SELECT cl_id2, ST_MakePolygon(ST_ExteriorRing((ST_Dump(ST_Buffer(ST_Buffer(geom, 20),-18))).geom)) geom FROM locoh_tc) t1",
                     "LEFT JOIN",
                     "(SELECT DISTINCT cluster_id, cl_name, cl_id2 FROM clusters) t2",
                     "ON t1.cl_id2 = t2.cl_id2"))

dbExecute(con, "CREATE TABLE boundaries_london(cluster_id text, cl_name text, geom geometry (polygon, 27700))")
dbExecute(con, paste("INSERT INTO boundaries_london(cluster_id, cl_name, geom)",
                     "SELECT t2.cluster_id, t2.cl_name, t1.geom FROM",
                     "(SELECT cl_id2, ST_MakePolygon(ST_ExteriorRing((ST_Dump(ST_Buffer(ST_Buffer(geom, 20),-18))).geom)) geom FROM locoh_london) t1",
                     "LEFT JOIN",
                     "(SELECT DISTINCT cluster_id, cl_name, cl_id2 FROM clusters) t2",
                     "ON t1.cl_id2 = t2.cl_id2"))

dbExecute(con, "CREATE TABLE boundaries_london_west(cluster_id text, cl_name text, geom geometry (polygon, 27700))")
dbExecute(con, paste("INSERT INTO boundaries_london_west(cluster_id, cl_name, geom)",
                     "SELECT t2.cluster_id, t2.cl_name, t1.geom FROM",
                     "(SELECT cl_id2, ST_MakePolygon(ST_ExteriorRing((ST_Dump(ST_Buffer(ST_Buffer(geom, 20),-18))).geom)) geom FROM locoh_london_west) t1",
                     "LEFT JOIN",
                     "(SELECT DISTINCT cluster_id, cl_name, cl_id2 FROM clusters) t2",
                     "ON t1.cl_id2 = t2.cl_id2"))

dbExecute(con, "CREATE TABLE boundaries_rp(cluster_id text, cl_name text, geom geometry (polygon, 27700))")
dbExecute(con, paste("INSERT INTO boundaries_rp(cluster_id, cl_name, geom)",
                     "SELECT t2.cluster_id, t2.cl_name, t1.geom FROM",
                     "(SELECT cl_id2, ST_MakePolygon(ST_ExteriorRing((ST_Dump(ST_Buffer(ST_Buffer(geom, 20),-18))).geom)) geom FROM locoh_rp2) t1",
                     "LEFT JOIN",
                     "(SELECT DISTINCT cluster_id, cl_name, cl_id2 FROM clusters) t2",
                     "ON t1.cl_id2 = t2.cl_id2"))

##### 5. Put everything together ###############################################################################################################

recl_boundaries <- st_read("~/Dropbox/liverpool/boundaries/recl_boundaries/new_boundaries_fin.shp", stringsAsFactors = F)
boundaries <- st_read("~/Dropbox/liverpool/boundaries/all_boundaries_fin.shp", stringsAsFactors = F)