library(sf)
library(RPostgreSQL)

locoh_postgis_query <- function(con, clusters_table, geom_field, clusters_field, points_id_field, cluster_id, k, rm_holes = F){
  st_read_db(con, query = paste0("WITH distinct_pnts AS",
                                  " (SELECT DISTINCT ON(", geom_field, ") geom, ", points_id_field, " id",
                                  " FROM ", clusters_table, " WHERE ", clusters_field, " = ", cluster_id, ")",
                                  ifelse (rm_holes, paste0(" SELECT ", cluster_id, " cluster_id, ST_Union(ST_MakePolygon(ST_ExteriorRing(geom))) geom",
                                                           " FROM ("),""),
                                  " SELECT ", cluster_id, " cluster_id, ST_Union(geom) geom", 
                                  " FROM", 
                                  " (SELECT t1.id, ST_Convexhull(ST_Collect(t2.geom)) geom", 
                                  " FROM distinct_pnts t1",
                                  " CROSS JOIN LATERAL", 
                                  " (SELECT geom FROM distinct_pnts t2", 
                                  " WHERE t2.id != t1.id", 
                                  " ORDER by t2.geom <#> t1.geom", 
                                  " LIMIT ", k, "-1) t2 GROUP BY t1.id) t3",
                                  ifelse(rm_holes, ") t4 ", "")))
}

# locoh_postgis_multi_clusters <- function(con, clusters_table, geom_field, clusters_field, points_id_field, cluster_ids, k, rm_holes = F){
#   st_read_db(con, paste0("WITH distinct_pnts AS",
#                          "(SELECT DISTINCT ON(", geom_field, ") geom, ", points_id_field, " id, ", clusters_field, " cl_id",
#                          " FROM ", clusters_table, " WHERE ", clusters_field, " IN (", paste0(cluster_ids, collapse = ","), "))",
#                          ifelse (rm_holes, paste0(" SELECT cl_id, ST_Union(ST_MakePolygon(ST_ExteriorRing(geom))) geom",
#                                                   " FROM ("),""),
#                          " SELECT t5.cl_id, ST_Union(t3.geom) geom FROM distinct_pnts t5",
#                          " CROSS JOIN LATERAL",
#                          " (SELECT t1.id, ST_Convexhull(ST_Collect(t2.geom)) geom",
#                          " FROM distinct_pnts t1",
#                          " CROSS JOIN LATERAL",
#                          " (SELECT geom FROM distinct_pnts t2",
#                          " WHERE t2.id != t1.id AND t2.cl_id = t5.cl_id",
#                          " ORDER by t2.geom <#> t1.geom LIMIT ", k, "-1) t2",
#                          " GROUP BY t1.id) t3",
#                          " GROUP BY t5.cl_id",
#                          ifelse(rm_holes, ") t4 GROUP BY cl_id", "")))
# }

locoh_poly <- function(con, clusters_table, geom_field, clusters_field, points_id_field, cluster_id, k, rm_holes){
  tryCatch(
    {
      sf_df <- locoh_postgis_query(con=con, clusters_table = clusters_table, geom_field = geom_field,
                          clusters_field = clusters_field, points_id_field = points_id_field,
                          cluster_id = cluster_id, k = k, rm_holes = rm_holes)
      # if the geometry is not polygon return null
      if (is.na(st_dimension(sf_df$geom)) | any(sapply(sf_df$geom, function(x) substr(st_as_text(x), 1, 1) != "P"))){
        return(NULL)
      } else {
        return(sf_df)
      }
    },
    # catch any errors from postgis
    error = function(err){
      warning(paste("The following error was issued for cluster = ", cluster_id, ", and k = ", k,": ", err))
      return(NULL)
    }
  )
}

.wrap_locoh_poly <- function(con, clusters_table, geom_field, clusters_field, points_id_field, cluster_id, k, rm_holes, points_nr){
  
  # postgis query for convex hull
  sf_df <- locoh_poly(con, clusters_table, geom_field, clusters_field, points_id_field, cluster_id, k, rm_holes)
  
  # if postgis_chull_query returned null try k = k + 1 for as long as k < number of points in the cluster of points
  if (is.null(sf_df)){
    warning_part_1 <- paste("It was not possible to create the convex hulls for cluster", cluster_id, "and k =", k)
    while(is.null(sf_df) & k < points_nr){
      k <- k + 1
      sf_df <- locoh_poly(con, clusters_table, geom_field, clusters_field, points_id_field, cluster_id, k, rm_holes)
    }
    # if still null abort
    if (is.null(sf_df)){
      warning(paste("It was not possible to create the local convex hull for cluster_id =", cluster_id))
      return(list(k = k, poly = NULL))
    }
    warning(paste(warning_part_1, ", returned local convex hull for k = ", k))
  }
  
  # return list, first element the k value used to produce convex hulls,
  # second element the loco data frame
  return(list(k = k, poly = sf_df))
}

get_locoh <- function(cluster_id, con, clusters_table, clusters_field, geom_field, points_id_field, rm_holes = F, k = NULL, k_max = Inf){
  
  points_nr <- as.integer(dbGetQuery(con, paste("SELECT count(DISTINCT ", geom_field, ") FROM ", clusters_table, " WHERE ", clusters_field, " = ", cluster_id, sep = "")))
  
  if (points_nr < 3){
    warning(paste0("there are less than 3 points for cluster ", cluster_id, ", returning empty polygon"))
    return(st_polygon())
  }
  
  if (k_max > points_nr){
    k_max <- points_nr
  }
  
  if (is.null(k)){
    k <- round(sqrt(points_nr))
  }
  
  if (k > k_max){
    k <- k_max
  } 
  
  # locoh <- chulls_to_locoh(clusters_table, cluster_id, k, points_nr, pct)
  temp_locoh <- .wrap_locoh_poly(con, clusters_table, geom_field, clusters_field, points_id_field, cluster_id, k, rm_holes, points_nr)
  
  # make sure that it's a single polygon
  while ((is.null(temp_locoh$poly) | length(temp_locoh$poly$geom) > 1) & k < k_max){
    k <- temp_locoh$k + 1
    temp_locoh <- .wrap_locoh_poly(con, clusters_table, geom_field, clusters_field, points_id_field, cluster_id, k, rm_holes, points_nr)
  }
  
  if (is.null(temp_locoh$poly)){
    warning(paste("It was not possible to create local convex hull for cluster =", cluster_id))
    return(st_polygon())
  }
  
  area_init <- sum(as.numeric(st_area(temp_locoh$poly))/10000)
  k <- temp_locoh$k + 1
  
  if (area_init < 1.1){
    area_threshold <- 0.001
  } else {
    area_threshold <- log(area_init) / 100
  }
  
  area <- area_init
  
  locoh <- temp_locoh
  
  # optimize locoh area: if the area has not plataued then try a higher k
  while (k < k_max & all(diff(area, 1) > area_threshold) & area[length(area)] < area_init + area_init / 3){
    temp_locoh <- .wrap_locoh_poly(con, clusters_table, geom_field, clusters_field, points_id_field, cluster_id, k, rm_holes, points_nr)
    if (! is.null(temp_locoh$poly)){
      new_area <- sum(as.numeric(st_area(temp_locoh$poly))/10000)
      if (new_area > area[length(area)]){
        locoh <- temp_locoh
      }
      area <- c(area, new_area)
    }
    k <- temp_locoh$k + 1
  }
  
  # do.call(rbind, lapply(locoh$shp_out, function(x) unlist(x)))
  if (length(locoh$poly$geom) > 1){
    return(st_union(locoh$poly))
  } else if (is.null(locoh$poly)){
    return(st_polygon())
  } else {
    return(locoh$poly)
  }
}
