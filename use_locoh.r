# library(parallel)

source("~/Dropbox/git/locoh/locoh.r")
source("~/Dropbox/git/postgis/postgis_functions.r")

con <- dbConnect(PostgreSQL(), dbname = "all_boundaries", user = "user", password = "pwd")

clusters_table = "buildings_points_intersect_all"; geom_field = "geom"; clusters_field = 'cl_id2';  points_id_field = "gid"

# create_postgis_locoh_func(con, clusters_table, geom_field, clusters_field, points_id_field)

# get cluster ids
# cluster_ids <- dbGetQuery(con, "select cl_id2 from buildings_points_intersect_all")$cl_id2
# quantile(cluster_ids, seq(0,1,0.1))
# 0%  10%  20%  30%  40%  50%  60%  70%  80%  90% 100% 
#  1  642  861 1107 1366 1592 1899 2218 2583 3015 3328

locoh1 <- lapply(1:642, function(cluster_id) postgis_locoh(con = con,
                                                             clusters_table = clusters_table,
                                                             clusters_field = clusters_field,
                                                             cluster_id = cluster_id))

locoh1_sfc <- st_as_sfc(do.call(rbind, lapply(1:642, function(x) st_as_text(locoh1[[x]]))))
locoh1_sf <- data.frame(cl_id2 = 1:642)
locoh1_sf$geometry <- locoh1_sfc
loco1_sf <- st_as_sf(locoh1_sf)


locoh2 <- lapply(643:861, function(cluster_id) postgis_locoh(con = con,
                                                             clusters_table = clusters_table,
                                                             clusters_field = clusters_field,
                                                             cluster_id = cluster_id))

locoh3 <- lapply(862:1107, function(cluster_id) postgis_locoh(con = con,
                                                             clusters_table = clusters_table,
                                                             clusters_field = clusters_field,
                                                             cluster_id = cluster_id))

locoh4 <- lapply(1108:1366, function(cluster_id) postgis_locoh(con = con,
                                                              clusters_table = clusters_table,
                                                              clusters_field = clusters_field,
                                                              cluster_id = cluster_id))

locoh5 <- lapply(1367:1592, function(cluster_id) postgis_locoh(con = con,
                                                               clusters_table = clusters_table,
                                                               clusters_field = clusters_field,
                                                               cluster_id = cluster_id))

locoh6 <- lapply(1593:1899, function(cluster_id) postgis_locoh(con = con,
                                                               clusters_table = clusters_table,
                                                               clusters_field = clusters_field,
                                                               cluster_id = cluster_id))

locoh7 <- lapply(1900:2218, function(cluster_id) postgis_locoh(con = con,
                                                               clusters_table = clusters_table,
                                                               clusters_field = clusters_field,
                                                               cluster_id = cluster_id))

locoh8 <- lapply(2219:2583, function(cluster_id) postgis_locoh(con = con,
                                                               clusters_table = clusters_table,
                                                               clusters_field = clusters_field,
                                                               cluster_id = cluster_id))

locoh9 <- lapply(2584:3014, function(cluster_id) postgis_locoh(con = con,
                                                               clusters_table = clusters_table,
                                                               clusters_field = clusters_field,
                                                               cluster_id = cluster_id,
                                                               k_max = 50))

saveRDS(locoh9, "~/Dropbox/liverpool/boundaries/locoh9.rds")

locoh_london <- postgis_locoh(con = con,
                              clusters_table = clusters_table,
                              clusters_field = clusters_field,
                              cluster_id = 3015,
                              k_max = 50)
saveRDS(locoh_london, "~/Dropbox/liverpool/boundaries/locoh_london.rds")


locoh10 <- lapply(3016:3328, function(cluster_id) postgis_locoh(con = con,
                                                               clusters_table = clusters_table,
                                                               clusters_field = clusters_field,
                                                               cluster_id = cluster_id))

drop_postgis_locoh_func(con, clusters_table)

setwd("~/Dropbox/liverpool/boundaries")
all_locoh <- list.files(".", "locoh*")

locoh_list <- lapply(all_locoh, readRDS)

locoh_text <- rapply(locoh_list, st_as_text, how = "unlist")

cluster_ids <- c(3016:3328, 1:642, 643:861, 862:1107, 1108:1366, 1367:1592, 1593:1899, 1900:2218, 2219:2583, 2584:3014, 3015)

polys_wkt <- do.call(rbind, lapply(1:length(locoh_list), function(x) do.call(rbind, lapply(locoh_list[[x]], function(the_poly) st_as_text(the_poly)))))

out <- as.data.frame(cbind(cluster_ids, polys_wkt), stringsAsFactors = F)
names(out) <- c("cl_id2", "geometry")

out <- st_as_sf(out, wkt = "geometry", crs = 27700)

st_write(out, "all_boundaries.shp", driver = "ESRI Shapefile")

######################################################################################################################################
#                                       Upload to DB, join with clusters, remove holes, buffer                                       #
######################################################################################################################################

##### 1. Upload to DB ################################################################################################################

import_or_append(con, getwd(), "all_boundaries.shp", 27700, "boundaries")
