source("~/Dropbox/git/locoh/locoh.r")

con <- dbConnect(PostgreSQL(), dbname = "all_boundaries", user = "user", password = "pwd")

clusters_table = "buildings_points_intersect_all"; geom_field = "geom"; clusters_field = "cl_id2";  points_id_field = "gid"

create_postgis_locoh_func(con = con, 
                          clusters_table = clusters_table,
                          geom_field = geom_field,
                          clusters_field = clusters_field,
                          points_id_field = points_id_field)


clusters <- st_read_db(con, clusters_table)
rp_ids <- unique(clusters$cl_id2)

locoh_rps <- lapply(rp_ids, function(cluster_id) postgis_locoh(con = con,
                                                               clusters_table = clusters_table,
                                                               clusters_field = clusters_field,
                                                               cluster_id = cluster_id,
                                                               k_max = 25))

locoh_sfc <- st_as_sfc(do.call(rbind, lapply(locoh_rps, function(x) st_as_text(x))), crs = 27700)
locoh_sf <- data.frame(cl_id2 = rp_ids)
locoh_sf$geometry <- locoh_sfc
locoh_sf <- st_as_sf(locoh_sf)
st_write(locoh_sf, "locoh_rps.shp", driver = "ESRI Shapefile")

drop_postgis_locoh_func(con, clusters_table = clusters_table)
