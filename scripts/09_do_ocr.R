

############### housekeeping ###############
pacman::p_load(tesseract,purrr,here,tictoc,magick,lubridate)
source(here("scripts","tagesschau","09a_crop_function.R")) # crop functions
# n_cores <- detectCores()-2
engine_ger <- tesseract("deu", # language option for engine
                    options = list(tessedit_char_blacklist = "©®{}}@_:!=|™<>()[]—~\\")) # blacklist
engine<-engine_ger
videos_index <- readRDS(here("data","videos_index.rds"))
dates_sequence <- as.Date(videos_index$dates)
dates_sequence_late <- dates_sequence[dates_sequence>"2014-04-24"]
dates_sequence_early <- dates_sequence[dates_sequence<"2014-04-19"]

############### First batch of videos (before format change) ##############

do_ocr_batch_early <- function(date,engine){
  tic()
  raw_arrays <- readRDS(here("frame_arrays",paste0(date,".RDS")))
  
  # need frame dim to tailor cropping rules to different resolutions
  frame_dim <- dim(raw_arrays[[1]])[[1]]
  
  cropped_top <- purrr::map(raw_arrays,
                            crop_small_top1,
                            frame_dim=frame_dim)
  cropped_line1 <- purrr::map(raw_arrays,
                              crop_small_top2,
                              frame_dim=frame_dim)
  cropped_line2 <- purrr::map(raw_arrays,
                              crop_small_location,
                              frame_dim=frame_dim)
  cropped_all <- purrr::map(raw_arrays,
                            crop,
                            frame_dim=frame_dim)
  raw_arrays<-NULL
  
  out_top <-purrr::map(cropped_top,magick::image_read)
  out_top <- purrr::map(out_top,magick::image_convert,type = 'Grayscale')
  out_top <- purrr::map(out_top,tesseract::ocr_data,engine=engine)
  
  out_line1 <-purrr::map(cropped_line1,magick::image_read)
  out_line1 <- purrr::map(out_line1,magick::image_convert,type = 'Grayscale')
  out_line1 <- purrr::map(out_line1,tesseract::ocr_data,engine=engine)
  
  out_line2 <-purrr::map(cropped_line2,magick::image_read)
  out_line2 <- purrr::map(out_line2,magick::image_convert,type = 'Grayscale')
  out_line2 <- purrr::map(out_line2,tesseract::ocr_data,engine=engine)
  
  out_all <-purrr::map(cropped_all,magick::image_read)
  out_all <- purrr::map(out_all,magick::image_convert,type = 'Grayscale')
  out_all <- purrr::map(out_all,tesseract::ocr_data,engine=engine)
  # toc()
  toc()
  return(list(out_top=out_top,
              out_line1=out_line1,
              out_line2=out_line2,
              out_all=out_all))
}


############# Second batch of vids (after format change) ###########

do_ocr_batch_late <- function(date,engine){
  tic()
  raw_arrays <- readRDS(here("frame_arrays",paste0(date,".RDS")))
  toc()
  # raw_arrays<-raw_arrays[1:30]
  
  cropped_top <- purrr::map(raw_arrays, crop_top)
  cropped_line1 <- purrr::map(raw_arrays, crop_line1)
  cropped_line2 <- purrr::map(raw_arrays, crop_line2)
  cropped_all <- purrr::map(raw_arrays, crop_all)
  raw_arrays<-NULL
  
  out_top <-purrr::map(cropped_top,magick::image_read)
  out_top <- purrr::map(out_top,magick::image_convert,type = 'Grayscale')
  out_top <- purrr::map(out_top,tesseract::ocr_data,engine=engine)
  
  out_line1 <-purrr::map(cropped_line1,magick::image_read)
  out_line1 <- purrr::map(out_line1,magick::image_convert,type = 'Grayscale')
  out_line1 <- purrr::map(out_line1,tesseract::ocr_data,engine=engine)
  
  out_line2 <-purrr::map(cropped_line2,magick::image_read)
  out_line2 <- purrr::map(out_line2,magick::image_convert,type = 'Grayscale')
  out_line2 <- purrr::map(out_line2,tesseract::ocr_data,engine=engine)
  
  out_all <-purrr::map(cropped_all,magick::image_read)
  out_all <- purrr::map(out_all,magick::image_convert,type = 'Grayscale')
  out_all <- purrr::map(out_all,tesseract::ocr_data,engine=engine)
  # toc()
  
  return(list(out_top=out_top,
              out_line1=out_line1,
              out_line2=out_line2,
              out_all=out_all))
}


##### run it ######

ocr_output_early <- map(dates_sequence_early,do_ocr_batch_early)
ocr_output_late <- map(dates_sequence_late,do_ocr_batch_late)
ocr_output <- c(ocr_output_early,ocr_output_late)
saveRDS(here("data","ocr_output.rds"))

######### Diagnostics (comment out) ########

# other useful diagnostic function
# 
# do_ocr_batch_2_nopar <- function(date,engine=engine){
#   # tic()
#   raw_arrays <- readRDS(here("frame_arrays",paste0(date,".RDS")))
#   # raw_arrays<-raw_arrays[1:30]
#   cropped_top <- purrr::map(raw_arrays, crop_top)
#   cropped_line1 <-  purrr::map(raw_arrays, crop_line1)
#   cropped_line2 <-  purrr::map(raw_arrays, crop_line2)
#   cropped_all <-  purrr::map(raw_arrays, crop_all)
#   raw_arrays <- NULL
#   
#   to_do_ocr<-list(cropped_top,cropped_line1,cropped_line2,cropped_all)
#   
#   out <- purrr::map(to_do_ocr,do_ocr,engine=engine)
#   # toc()
#   
#   return(out)
# }
# 



# simple func useful for diagnostics
# do_ocr<- function(cropped,engine=engine){
#   out <-purrr::map(cropped,magick::image_read)
#   out <- purrr::map(out,magick::image_convert,type = 'Grayscale')
#   out <- purrr::map(out,ocr_data,engine=engine)
# }




# 
# engine_eng <- tesseract("eng", 
#                         options = list(tessedit_char_blacklist = "©®{}}@_:!=|™<>()[]—~\\")) # blacklisted chars
# end_date<-'2019-01-01'       # '2017-01-01'
# start_date<-'2014-04-19'        # '2019-01-01'
# dates_sequence_orig<-seq(ymd(start_date),ymd(end_date), by = '1 day') #'2014-01-01'
# dates_sequence_true <- dates_sequence[dates_sequence!="2014-04-24"&dates_sequence!="2016-07-22"]
# 
# videos_index <- readRDS(here("data","videos_index.rds"))
# dates_sequence <- as.Date(videos_index$dates)
# dates_sequence_late <- dates_sequence[dates_sequence>"2014-04-24"]
# dates_sequence_early <- dates_sequence[dates_sequence<"2014-04-19"]
# dates_sequence_early_1_100 <- dates_sequence_early[1:100]
# dates_sequence_early_101_200 <- dates_sequence_early[101:200]
# dates_sequence_early_201_322 <- dates_sequence_early[201:322]
# 
