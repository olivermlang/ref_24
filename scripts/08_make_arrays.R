########## Make arrays ##################3

# housekeeping
# no_cores <- availableCores() - 6 # only comment out for parallel
# plan(multicore, workers = no_cores)
frames_dir <- here::here("frames")
array_dir <- here::here("frame_arrays")
vid_dirs_rel <- list.files(here::here("frames"),full.names = FALSE) # relative paths to frames

image_array_func <- function(vid_dir_rel, frames_dir, array_dir) { 
  # vid_dir_rel = relative path to video from frame directory (frames_dir) and array dir (array_dir)
  image_array_list <- lapply(list.files(paste(frames_dir, # load all frames from subdirectory of vid_dir_rel frames directory
                                              vid_dir_rel,
                                              sep = "/"),
                                        full.names = TRUE),
                             imager::load.image)  # load as array
  print(pryr::mem_used()) # check size
  saveRDS(image_array_list, file = paste(array_dir, paste0(vid_dir_rel,".RDS"), sep = "/")) # save to target directory
}
# loop over all videos
purrr::walk(vid_dirs_rel, image_array_func ,frames_dir = frames_dir, array_dir = array_dir)



# 2:23
# 2:28

## wrong thing get scraped?


