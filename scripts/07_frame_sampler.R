pacman::p_load(purrr, here)
############## sample images from vid files ###############3

# two different ocr processes: one for start - 2014-04-18, another for 2014-04-19 - end

vids <- list.files(here::here("vids"),full.names = TRUE)
vid_names <- stringr::str_remove(list.files(here::here("vids")),".mp4")
vid_names <- vid_names[!(vid_names %in% list.files(here::here("frames")))]
map(vid_names, function(x) dir.create(paste(here::here("frames"),x,sep = "/")))
vid_dirs <- list.files(here::here("frames"),full.names = TRUE)


frame_sampler <- function(vid_name){
  av::av_video_images(paste(here::here("vids"),
                        paste0(vid_name,".mp4"),
                        sep="/"),
                  destdir = paste(here::here("frames"),vid_name,sep = "/"),
                  format = "jpg",
                  fps = .5) #extract images, fps of .5 = sampling rate = one frame every 2 seconds
}


vid_counter <- unlist(purrr::map(vid_dirs,function(x) length(list.files(x))))
vid_names_to_do <- vid_names[vid_counter==0] # only useful if you want to divide up into multiple jobs
future.apply::future_lapply(vid_names_to_do,frame_sampler)
