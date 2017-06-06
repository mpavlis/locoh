library(sf)
library(RPostgreSQL)

create_postgis_locoh_func <- function(con, clusters_table, geom_field, clusters_field, points_id_field){
  
  query <- paste("CREATE OR REPLACE FUNCTION st_locoh_", clusters_table, "(cluster_id integer, knn integer)",
                 " RETURNS TABLE(focal_gid integer, geom geometry, area double precision, knn_gid integer []) AS $$",
                 " DECLARE",
                 " i integer;",
                 " BEGIN",
                 " FOR i IN SELECT DISTINCT ON(", clusters_table,".", geom_field,") ", clusters_table,".", points_id_field,
                 " FROM ", clusters_table, " WHERE ", clusters_table, ".", clusters_field, "= $1",
                 " LOOP",
                 " BEGIN RETURN QUERY",
                 " WITH cluster_sele AS",
                 " (SELECT DISTINCT ON(",clusters_table,".", geom_field,") ", clusters_table, ".", geom_field, " AS geom,", clusters_table, ".", points_id_field, " AS gid", 
                 " FROM ", clusters_table, " WHERE ", clusters_table, ".", clusters_field, " = $1),",
                 " focal_pnt AS",
                 " (SELECT cluster_sele.gid, cluster_sele.geom FROM cluster_sele WHERE cluster_sele.gid = i),",
                 " knn_pnts AS",
                 " (SELECT cluster_sele.gid AS gid, cluster_sele.geom AS geom FROM cluster_sele, focal_pnt",
                 " WHERE cluster_sele.gid != i ORDER BY cluster_sele.geom <#> focal_pnt.geom LIMIT $2 - 1),",
                 " c_hull AS",
                 " (SELECT focal_pnt.gid AS focal_gid, ST_ConvexHull(ST_Collect(cluster_sele.geom)) AS geom, array_agg(DISTINCT knn_pnts.gid) AS knn_gid",
                 " FROM cluster_sele, focal_pnt, knn_pnts",
                 " WHERE cluster_sele.gid IN (focal_pnt.gid, knn_pnts.gid) GROUP BY focal_pnt.gid)",
                 " SELECT c_hull.focal_gid, c_hull.geom, ST_Area(c_hull.geom)/10000 AS area, c_hull.knn_gid FROM c_hull;",
                 " END;",
                 " END LOOP;",
                 " END $$",
                 " LANGUAGE plpgsql;", sep = "")
  
  dbExecute(con, query)
}

drop_postgis_locoh_func <- function(con, clusters_table){
  dbExecute(con, paste0("DROP FUNCTION st_locoh_", clusters_table, "(cluster_id integer, knn integer)"))
}

#################################### 2. postgis_chull_query ###############################################
# use postgis to create a convex hull from each point to its closest knn - 1 neighbours within a cluster
# the function will check whether a polygon geometry was returned, if not it will return null
# if polygons it returns a data frame as: id of focal point, geometry of convex hull and ids of knn - 1
# the last column is produced from a postgres array and is imported as factor in R

postgis_chull_query <- function(con, clusters_table, cluster_id, k){
  function_name <- paste("st_locoh", clusters_table, sep = "_")
  tryCatch(
    {
      Df <- dbGetQuery(con, paste("SELECT focal_gid, knn_gid, ST_AsText(geom) AS geom FROM ", function_name, "(", cluster_id, ",", k, ") ORDER BY area", sep = ""))
      # if the geometry is not polygon return null
      if (any(sapply(Df$geom, function(x) substr(x, 1, 1) != "P"))){
        return(NULL)
      } else {
        return(Df)
      }
    },
    # catch any errors from postgis
    error = function(err){
      warning(paste("The following error was issued for cluster=", cluster_id, ", and k=", k,":", err))
      return(NULL)
    }
  )
}

######################################### 3. postgis_chull ###################################################
# Based on the function postgis_chull_query use postgis to return a convex hull for each point within a cluster of points
# if the provided number of nearest neighbours (k) does not return a polygons geometry try higher k
# returns a list with two elements: the final k value and the data frame as returned from function postgis_loco_query

postgis_chull <- function(con, clusters_table, cluster_id, k, points_nr){
  
  # postgis query for convex hull
  chulls <- postgis_chull_query(con, clusters_table, cluster_id, k)
  
  # if postgis_chull_query returned null try k = k + 1 for as long as k < number of points in the cluster
  if (is.null(chulls)){
    warning_part_1 <- paste("It was not possible to create the convex hulls for cluster", cluster_id, "and k =", k)
    while(is.null(chulls) & k < points_nr){
      k <- k + 1
      chulls <- postgis_chull_query(con, clusters_table, cluster_id, k)
    }
    # if still null abort
    if (is.null(chulls)){
      warning(paste("It was not possible to create the convex hulls for cluster_id = ",cluster_id))
      return(NULL)
    }
    warning(paste(warning_part_1, "returned convex hulls for k =", k))
  }
  
  # return list, first element the k value used to produce convex hulls,
  # second element the loco data frame
  return(list(k = k, chulls = chulls))
}

####################################### 4. chulls_to_locoh ###################################################
# Using the above three functions take the postgis convex hull polygons and return a list with two elements:
# 1) k = the number of nearest neighbours and 2) poly = a spatial polygons data frame created from the postgis polygons
# the polygons are selected so as the cumulative probability of points within the catchment area is equal to or lower than the pct threshold

chulls_to_locoh <- function(con, clusters_table, cluster_id, k, points_nr){
  
  chulls <- postgis_chull(con, clusters_table, cluster_id, k, points_nr)
  
  if (is.null(chulls)) return(NULL)
  
  k <- chulls$k
  chulls <- chulls$chulls
  
  if (anyDuplicated(chulls$geom) > 0){
    idx_rm <- which(duplicated(chulls$geom))
    chulls <- chulls[-idx_rm, ]
  }
  
  tryCatch(
    {
      polys <- st_cast(st_union(st_as_sfc(chulls$geom)), "POLYGON")
      # polys <- gUnaryUnion(SpatialPolygons(polys))
    },
    error = function(err){
      warning(paste("It was not possible to create local convex hull for cluster=", cluster_id, ", trying next k, error received:", err))
    }
  )
  
  if (is(polys, "sfc")){
    return(list(k = k, shp_out=polys))
  } else {
    return(NULL)
  }
}


############################################### Main Function ##########################################
postgis_locoh <- function(con, clusters_table, clusters_field, cluster_id, k_max = Inf){
  
  points_nr <- as.integer(dbGetQuery(con, paste("SELECT count(DISTINCT ", geom_field, ") FROM ", clusters_table, " WHERE ", clusters_field, " = ", cluster_id, sep = "")))
  
  k <- round(sqrt(points_nr))
  
  if (k > k_max){
    k <- k_max
  } 
  
  # locoh <- chulls_to_locoh(clusters_table, cluster_id, k, points_nr, pct)
  temp_locoh <- chulls_to_locoh(con, clusters_table, cluster_id, k, points_nr)
  
  # make sure that it's a single polygon
  while ((is.null(temp_locoh) | length(temp_locoh$shp_out) > 1) & k < points_nr){
    k <- temp_locoh$k + 1
    temp_locoh <- chulls_to_locoh(con, clusters_table, cluster_id, k, points_nr)
  }
  
  if (is.null(temp_locoh)){
    warning(paste("It was not possible to create local convex hull for cluster =", cluster_id))
    return(NULL)
  }
  
  area_start <- st_area(temp_locoh$shp_out)/10000
  k_start <- temp_locoh$k
  
  if (area_start < 1.1){
    area_threshold <- 0.001
  } else {
    area_threshold <- log(area_start) / 100
  }
  
  k <- k_start + 1
  area <- area_start
  
  locoh <- temp_locoh
  
  # optimize locoh area: if the area has not plataued then try a higher k
  while (k < points_nr & k < k_max & all(diff(area, 1) > area_threshold) & area[length(area)] < area_start + area_start / 3){
    temp_locoh <- chulls_to_locoh(con, clusters_table, cluster_id, k, points_nr)
    if (! is.null(temp_locoh) ){
      area <- c(area, st_area(temp_locoh$shp_out)/10000)
      k <- temp_locoh$k + 1
      locoh <- temp_locoh
    } else {
      next()
    }
  }
  
  # do.call(rbind, lapply(locoh$shp_out, function(x) unlist(x)))
  if (length(locoh$shp_out) > 1){
    st_cast(locoh$shp_out, "MULTIPOLYGON")
  } else {
    locoh$shp_out
  }
  
}

