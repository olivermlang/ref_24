# Crop function

crop <- function(x ,frame_dim){
  if(frame_dim==960){
    out <- imager::imsub(x, y < 125 & y > 50, x < 550) # inequalities indicate where on x and y to cut
  }
  if(frame_dim==1280){ # if video is higher definition, use different cropping rules
    out <- imager::imsub(x, y < 160 & y > 60, x < 700)
  }
  return(out)
}

crop_small_top1 <- function(x,frame_dim){
  if(frame_dim==1280){
    out <- imager::imsub(x, y < 120 & y > 60, x < 740)
  }
  if(frame_dim==960){
    out <- imager::imsub(x, y < 88 & y > 41, x < 545)
  }
  return(out)
}

crop_small_top2 <- function(x,frame_dim){
  if(frame_dim==1280){
    out <- imager::imsub(x, y < 165 & y > 110, x < 740)
  }
  if(frame_dim==960){
    out <- imager::imsub(x, y < 125 & y > 81, x < 545)
  }
  return(out)
}

crop_small_location <- function(x,frame_dim){
  if(frame_dim==1280){
    out <- imager::imsub(x, y < 570 & y > 510, x < 680)
  }
  if(frame_dim==960){
    out <- imager::imsub(x, y < 430 & y > 388, x < 545)
  }
  return(out)
}

crop_small_date <- function(x,frame_dim){
  if(frame_dim==1280){
    out <- imager::imsub(x, y < 630 & y > 585, x < 500 & x > 50)
  }
  if(frame_dim==960){
    out <- imager::imsub(x, y < 483 & y > 445, x < 369 & x > 62)
  }
  return(out)
}








# date <- "2014-04-10"


# later dates 
crop_top <- function(x){
  out_top <- imager::imsub(x, y < 460 & y > 410, x < 680 & x > 50)
  # plot(out_top)
}
crop_line1 <- function(x){
  out_line1 <- imager::imsub(x, y < 528 & y > 455, x < 720, x > 25)
  # plot(out_line1)
}
crop_line2 <- function(x){
  out_line2 <- imager::imsub(x, y < 598 & y > 520, x < 720, x > 25)
  # plot(out_line2)
}

crop_all <- function(x){
  out_all <- imager::imsub(x, y < 598 & y > 420, x < 720 & x > 25)
}



# og
# crop_top <- function(x){
#   out_top <- imager::imsub(x, y < 455 & y > 417, x < 680 & x > 50)
#   # plot(out_top)
# }
# crop_line1 <- function(x){
#   out_line1 <- imager::imsub(x, y < 526 & y > 460, x < 720, x > 25)
#   # plot(out_line1)
# }
# crop_line2 <- function(x){
#   out_line2 <- imager::imsub(x, y < 590 & y > 527, x < 720, x > 25)
#   # plot(out_line2)
# }




########## test ############3


# 
# date1 <- "2016-04-27" # need to crop lower
# prac1 <- readRDS(here::here("frame_arrays",paste0(date1,".RDS")))
# cropped_top1 <- purrr::map(prac1, function(x) crop_top(x) )
# cropped_line11 <- purrr::map(prac1, function(x) crop_line1(x) )
# cropped_line21 <- purrr::map(prac1, function(x) crop_line2(x) )
# plot(cropped_top1[[14]])
# plot(cropped_line11[[14]])
# plot(cropped_line21[[14]])
# prac1 <- NULL
# 
# date2 <- "2017-04-27" # cropped well
# prac2 <- readRDS(here::here("frame_arrays",paste0(date2,".RDS")))
# cropped_top2 <- purrr::map(prac2, function(x) crop_top(x) )
# cropped_line12 <- purrr::map(prac2, function(x) crop_line1(x) )
# cropped_line22 <- purrr::map(prac2, function(x) crop_line2(x) )
# plot(cropped_top2[[14]])
# plot(cropped_line12[[14]])
# plot(cropped_line22[[14]])
# prac2 <- NULL
# 
# 
# 
# date3 <- "2018-09-10" # cropped well
# prac3 <- readRDS(here::here("frame_arrays",paste0(date3,".RDS")))
# cropped_top3 <- purrr::map(prac3, function(x) crop_top(x) )
# cropped_line13 <- purrr::map(prac3, function(x) crop_line1(x) )
# cropped_line23 <- purrr::map(prac3, function(x) crop_line2(x) )
# plot(cropped_top3[[14]])
# plot(cropped_line13[[14]])
# plot(cropped_line23[[14]])
# 
# date4 <- "2018-01-10" # need to crop way higher
# prac4 <- readRDS(here::here("frame_arrays",paste0(date4,".RDS")))
# cropped_top4 <- purrr::map(prac4, function(x) crop_top(x) )
# cropped_line14 <- purrr::map(prac4, function(x) crop_line1(x) )
# cropped_line24 <- purrr::map(prac4, function(x) crop_line2(x) )
# plot(cropped_top4[[14]])
# plot(cropped_line14[[14]])
# plot(cropped_line24[[14]])
# prac4 <- NULL
# 
# 
# 
# date5 <- "2017-08-02" # cropped well
# prac5 <- readRDS(here::here("frame_arrays",paste0(date5,".RDS")))
# cropped_top5 <- purrr::map(prac5, function(x) crop_top(x) )
# cropped_line15 <- purrr::map(prac5, function(x) crop_line1(x) )
# cropped_line25 <- purrr::map(prac5, function(x) crop_line2(x) )
# plot(cropped_top5[[14]])
# plot(cropped_line15[[14]])
# plot(cropped_line25[[14]])
# prac5 <- NULL
# 
# 
# date6 <- "2015-01-01" # crop just a wee bit higher
# prac6 <- readRDS(here::here("frame_arrays",paste0(date6,".RDS")))
# cropped_top6 <- purrr::map(prac6, function(x) crop_top(x) )
# cropped_line16 <- purrr::map(prac6, function(x) crop_line1(x) )
# cropped_line26 <- purrr::map(prac6, function(x) crop_line2(x) )
# plot(cropped_top6[[14]])
# plot(cropped_line16[[14]])
# plot(cropped_line26[[14]])
# prac6 <- NULL
# 
# 
# date7 <- "2016-02-01" # crop lower
# prac7 <- readRDS(here::here("frame_arrays",paste0(date7,".RDS")))
# cropped_top7 <- purrr::map(prac7, function(x) crop_top(x) )
# cropped_line17 <- purrr::map(prac7, function(x) crop_line1(x) )
# cropped_line27 <- purrr::map(prac7, function(x) crop_line2(x) )
# plot(cropped_top7[[14]])
# plot(cropped_line17[[14]])
# plot(cropped_line27[[14]])
# prac7 <- NULL
# 
# date8 <- "2018-03-21" # cropped well
# prac8 <- readRDS(here::here("frame_arrays",paste0(date8,".RDS")))
# cropped_top8 <- purrr::map(prac8, function(x) crop_top(x) )
# cropped_line18 <- purrr::map(prac8, function(x) crop_line1(x) )
# cropped_line28 <- purrr::map(prac8, function(x) crop_line2(x) )
# plot(cropped_top8[[14]])
# plot(cropped_line18[[14]])
# plot(cropped_line28[[14]])
# prac8 <- NULL
# 
# 
# date9 <- "2017-02-02" # cropped well
# prac9 <- readRDS(here::here("frame_arrays",paste0(date9,".RDS")))
# cropped_top9 <- purrr::map(prac9, function(x) crop_top(x) )
# cropped_line19 <- purrr::map(prac9, function(x) crop_line1(x) )
# cropped_line29 <- purrr::map(prac9, function(x) crop_line2(x) )
# plot(cropped_top9[[14]])
# plot(cropped_line19[[14]])
# plot(cropped_line29[[14]])
# prac9 <- NULL
# 
# date10 <- "2018-05-11" # well cropped
# prac10 <- readRDS(here::here("frame_arrays",paste0(date10,".RDS")))
# cropped_top10 <- purrr::map(prac10, function(x) crop_top(x) )
# cropped_line110 <- purrr::map(prac10, function(x) crop_line1(x) )
# cropped_line210 <- purrr::map(prac10, function(x) crop_line2(x) )
# plot(cropped_top10[[14]])
# plot(cropped_line110[[14]])
# plot(cropped_line210[[14]])
# prac10 <- NULL
