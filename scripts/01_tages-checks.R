pacman::p_load(data.table,lubridate,here,stringr,dplyr,readr,jsonlite,purrr,av,tesseract,
               googleCloudStorageR,magick,parallel,janitor)

############### Clean metadata ############### 

tages_vids <- list.files("/Volumes/4tb_sam/tages/",full.names = T)
# load in all vid and json input
vidoutput <- tages_vids |> 
  stringr::str_subset(".json$|.wav$|.webm$|.mkv$|.mp4$|.flac$")
all_json_names <- tages_vids |> 
  str_subset(".info.json")

# target dates
date_seq <- 
  seq(as.Date("2019-01-02"),as.Date("2023-01-01"),by=1)



all_jsons <- map(all_json_names,jsonlite::read_json)
names(all_jsons) <- all_json_names



all_meta_essential <- 
  map(all_jsons, ~.x[c("id","title","thumbnail",
                      "description","duration",
                      "channel_id","channel_url",
                      "view_count","webpage_url",
                      "categories","live_status",
                      "upload_date",
                      "resolution","fps","width","height",
                      "dynamic_range","vcodec","aspect_ratio",
                      "audio_channels",
                      "playlist","playlist_count","playlist_id",
                      "playlist_title","n_entries")]) %>% 
  map(discard, is.null) %>%  # deal with empties
  map(discard, ~length(.x)==0)

meta_df <- rbindlist(all_meta_essential, use.names=T, fill=T, idcol="fpath")
meta_df <- select(meta_df,any_of(c("id","title","thumbnail",
                                   "description","duration",
                                   "channel_id","channel_url",
                                   "view_count","webpage_url",
                                   "categories","live_status",
                                   "upload_date","fpath",
                                   "resolution","fps","width","height",
                                   "dynamic_range","vcodec","aspect_ratio",
                                   "audio_channels",
                                   "playlist","playlist_count","playlist_id",
                                   "playlist_title","n_entries"))) %>% as.data.frame()


meta_df$fpath_rel <- str_remove(meta_df$fpath,"/Volumes/4tb_sam/tages//") %>% str_remove(".info.json")
meta_df$broadcast_date <- str_extract(meta_df$title, "[:digit:]{1,2}.[:digit:]{1,2}.[:digit:]{2,4}")
meta_df$broadcast_date  <- gsub("(\\d{1,2}\\.\\d{1,2}\\.)(\\d{2})$", "\\120\\2", meta_df$broadcast_date )
meta_df$broadcast_date_fmt <- as.Date(meta_df$broadcast_date, format = "%d.%m.%Y")
broadcast_date_fmt <- meta_df$broadcast_date_fmt[!is.na(meta_df$broadcast_date_fmt)&
                                                   meta_df$broadcast_date_fmt<=max(date_seq)&
                                                   meta_df$broadcast_date_fmt>=min(date_seq)]

meta_df <- filter(meta_df,!is.na(broadcast_date_fmt)&broadcast_date_fmt%in%date_seq) %>% 
  filter(file.exists(str_replace(fpath, ".info.json",".mp4"))&
           file.exists(str_replace(fpath, ".info.json",".flac")))

nrow(get_dupes(meta_df,broadcast_date_fmt))==0

############### Merge and prep data for transcription ############### 

# x <- c()
# for (i in seq_along(meta_df$fpath)) {
#   x[i] <- file.exists(str_replace(meta_df$fpath[i], ".info.json",".mp4"))
# }
# meta_df$title[which(x==FALSE)]
# meta_df <- meta_df[which(x==TRUE),]
btages_remedial <- list.files("/Volumes/4tb_sam/tages_remedial/")%>% str_subset(".flac") %>% 
  as.Date(format="%Y%m%d")

for (i in seq_along(btages_remedial)){
  
}

# broadcast_date <- meta_df$broadcast_date[!is.na(meta_df$broadcast_date)]
# load in all vids not covered in original scrape
btages_remedial <- list.files("/Volumes/4tb_sam/tages_remedial/")%>% str_subset(".flac") %>% 
  as.Date(format="%d.%m.%Y")
!any(duplicated(btages_remedial))

# btages_remedial_totrans <- list.files("/Volumes/4tb_sam/tages_remedial/")%>% str_subset(".mp4$")
# btages_remedial_trans <- list.files("/Volumes/4tb_sam/tages_remedial/")%>% str_subset(".mp4$") %>% 
#   as.Date(format="%d.%m.%Y") %>% paste0(.,".mp4")
# 
# file.copy(paste0("/Volumes/4tb_sam/tages_remedial/",btages_remedial_totrans), here("data_large","clean_audiovisual",btages_remedial_trans))

all_vids <- c(meta_df$broadcast_date_fmt,btages_remedial)




#have videos for every date in sequence? other sanity checks
which(duplicated(all_vids))
(length(meta_df$broadcast_date_fmt)+length(btages_remedial))==length(date_seq)
all(date_seq%in%all_vids)
all(all_vids%in%date_seq)
all(sort(all_vids)==sort(date_seq))
!any(is.na(all_vids))
all_vids <- c(btages_remedial,broadcast_date_fmt)

meta_df_totscribe <- meta_df

dir.create(here("data_large","clean_audiovisual"))
toscrapeflac <- c(as.Date(NA))
toscrapemp4 <- c(as.Date(NA))
for (i in 1:nrow(meta_df_totscribe)) {
  out_flac <- paste0(meta_df_totscribe$broadcast_date_fmt[i],".flac")
  out_mp4  <- paste0(meta_df_totscribe$broadcast_date_fmt[i],".mp4")
  in_flac <- str_replace(meta_df_totscribe$fpath[i],'.info.json','.flac') 
  in_mp4  <- str_replace(meta_df_totscribe$fpath[i],'.info.json','.mp4')
  # print(file.exists(in_flac))
  
  if(!file.exists(in_flac)) {
    # file.remove(meta_df_totscribe$fpath[i])
    # file.remove(in_mp4)
    toscrapeflac <- c(toscrapeflac,meta_df_totscribe$broadcast_date_fmt[i])
  }
  if(!file.exists(in_mp4)){
    # file.remove(meta_df_totscribe$fpath[i])
    # file.remove(in_flac)
    toscrapemp4 <- c(toscrapemp4,meta_df_totscribe$broadcast_date_fmt[i])
  }
  # 
  file.copy(from=in_mp4,to=here("data_large","clean_audiovisual",out_mp4))
  file.copy(from=in_flac,to=here("data_large","clean_audiovisual",out_flac))
  print(i)
}

for (i in seq_along(btages_remedial)) {
  out_flac <- paste0(btages_remedial[i],".flac")
  out_mp4  <- paste0(btages_remedial[i],".mp4")
  in_flac <- paste0("/Volumes/4tb_sam/tages_remedial/", paste0(format(as.Date(btages_remedial[i]),"%d.%m.%Y"),".flac") )
  in_mp4  <- paste0("/Volumes/4tb_sam/tages_remedial/",paste0(format(as.Date(btages_remedial[i]),"%d.%m.%Y"),".mp4") )
  # print(file.exists(in_flac))
  file.copy(from=in_mp4,to=here("data_large","clean_audiovisual",out_mp4))
  file.copy(from=in_flac,to=here("data_large","clean_audiovisual",out_flac))
  print(i)
}


# clean_flac <- list.files(here("data_large","clean_audiovisual"),full.names = F) %>% 
#   str_subset('.flac$') %>% str_remove(".flac")
# clean_mp4 <-  list.files(here("data_large","clean_audiovisual"),full.names = F) %>% 
#   str_subset('.mp4$') %>% str_remove( '.mp4')
# 
# 
# 
# tag <- paste0('https://www.tagesschau.de/archiv/sendungen?datum=',
#               date_seq[!date_seq%in%clean_flac]) %>% str_sub(1,-3) %>% paste0('01')
# 
# for(i in seq_along(tag)){ 
#   browseURL(tag[i])
#   Sys.sleep(2)
# }
# 
# walk(tag,browseURL)

###
# get cloud objects
bucket <- gcs_get_global_bucket()
bucket_objs <- gcs_list_objects(bucket = bucket,
                                delimiter=FALSE,
                                detail = c("summary", "more", "full"),
                                prefix = NULL) 

tages_obj <- str_subset(bucket_objs$name,"tagesschau.*flac")
tages_rem <- str_subset(bucket_objs$name,"tages_remedial.*flac")


# inmeta <- str_remove(tages_obj,'^tagesschau/')%in%paste0(meta_df_totscribe$fpath_rel,".flac")
# walk(paste0('gs://tscribe_audio/',tages_obj[!inmeta]),gcs_delete_object)
# 


############### Do transcribing ############### 

# virtual env w/ python3.12 
venv_path <- "~/Documents/censorship/tv/data/transcription/python_stuff/transcribe-vids/bin/activate"
venv_activiation    <- paste0('source ',venv_path)
# add arguments to script
python_tscribe_base <- 'python3 TSCRIBEPYPATH FULLPATH JSONBASEDIR CHANNELPROGRAMID'
# broadcast_date_1 <- date_seq[i]

# transcription function
tscribe_tag <- function(broadcast_date_1){

  if( file.exists(here("data_large","tscripts_clean",paste0(broadcast_date_1,".rds")))){
    print("all done")
  } else {
    dateseq_int <- format(broadcast_date_1,"%Y%m%d")
    dateseq_pt  <- format(broadcast_date_1, "%d.%m.%Y")
    
    # if (any(str_detect(list.files(here("data_large","tscripts")),
    #                               paste(dateseq_int,dateseq_pt,
    #                                     collapse="|")))) {
    #   print(paste0(broadcast_date_1," -- json already present"))
    #   next
    # }
    if(any(str_detect(tages_rem, dateseq_pt))){
      full_path <- str_subset(tages_rem, dateseq_pt)
      print(full_path)
      
      python_tscribe_correct <- python_tscribe_base %>% 
        str_replace("FULLPATH",        full_path) %>% 
        str_replace("JSONBASEDIR",    here("data_large","tscripts_oldapi") ) %>% 
        str_replace("TSCRIBEPYPATH",  here("scripts","tages_tscribe_oldapi.py")) %>% 
        str_replace("CHANNELPROGRAMID","tages_remedial")
      
      tscribe_command <- paste(venv_activiation, python_tscribe_correct, sep="; ")
      tscribe_terminal_output <- safely(function() {
        system(tscribe_command, intern = TRUE, wait = TRUE)
      })()
      if (is.null(tscribe_terminal_output$error)) {
        output        <- "Success"
        error_message <- ""
      } else {       # Handle the error message
        error_message <- tscribe_terminal_output$error$message
        output        <- ""
      }
    } else {
      # Note: dates listed on file paths are date video was uploaded to YT
      # != BROADCAST DATE WHICH IS STORED IN "broadcast_date_fmt" VAR
      if (broadcast_date_1%in%meta_df_totscribe$broadcast_date_fmt) {
        id <- meta_df_totscribe$id[meta_df_totscribe$broadcast_date_fmt==broadcast_date_1]
        
        full_path <- str_subset(tages_obj, id)
        print(full_path)
        
        python_tscribe_correct <- python_tscribe_base %>% 
          str_replace("FULLPATH",        full_path) %>% 
          str_replace("JSONBASEDIR",    here("data_large","tscripts_oldapi") ) %>% 
          str_replace("TSCRIBEPYPATH",  here("scripts","tages_tscribe_oldapi.py")) %>% 
          str_replace("CHANNELPROGRAMID","tagesschau")
        
        # combine commands and run
        tscribe_command <- paste(venv_activiation, python_tscribe_correct, sep="; ")
        tscribe_terminal_output <- safely(function() {
          system(tscribe_command, intern = TRUE, wait = TRUE)
        })()
        if (is.null(tscribe_terminal_output$error)) {
          output        <- "Success"
          error_message <- ""
        } else {       # Handle the error message
          error_message <- tscribe_terminal_output$error$message
          output        <- ""
        }
        print(tscribe_terminal_output)
      }
    }
    
    json_input <- here("data_large","tscripts_oldapi",str_replace(full_path,".flac",".transcript.json")) %>% 
      str_remove("tagesschau|tages_remedial") %>% jsonlite::read_json()
    
    
    
    input_file <- names(json_input$results)
    billed_duration <- json_input$totalBilledTime
    
    if (billed_duration=="0s") {
      tscript_df <- "error"
    } else {
      
      
      results      <- json_input$results
      tscript_df <- map(json_input$results,pluck,'alternatives') %>% 
        map(pluck,1) %>% 
        map(pluck,'words') %>% 
        map(rbindlist,use.names = T,fill=T) %>% 
        rbindlist(use.names = T,fill=T)
      
      print(paste(tscript_df$word[1:30],collapse=' '))
      # wrdlst_text_1       <- map(alternatives,     ~ifelse(length(.x)!=0,pluck(.x[[1]],"words"),     "")) 
      # wrdlst_text_1
      # confid_text_1       <- map(alternatives,     ~ifelse(length(.x)!=0,pluck(.x[[1]],"confidence"),"")) %>% unlist() %>% as.numeric()
      # output_text_1       <- map_chr(alternatives, ~ifelse(length(.x)!=0,pluck(.x[[1]],"transcript"),""))
      # 
      # tscript_df <- data.frame(text_1 =output_text_1,
      #                          conf_1 =confid_text_1,
      #                          no_alts=alternatives_length)
      # some videos uploaded w/o sound, deal with these here
      if (nrow(tscript_df)>0){
        tscript_df$anyaudio<-1; tscript_df$sound<-1; tscript_df$input_file<-input_file; tscript_df$billed_duration<-billed_duration
      } else {
        tscript_df[1,] <- NA;tscript_df$sound<-NA; tscript_df$input_file <- input_file; tscript_df$billed_duration <- billed_duration
        if (billed_duration=="30s") {
          tscript_df$anyaudio <- 0
        }
      }
      billed_duration
      saveRDS(tscript_df, here("data_large","tscripts_clean",paste0(broadcast_date_1,".rds")))
    }
  }
}


parallel::mclapply(date_seq,tscribe_tag,mc.cores=10)

############### Get and clean transcripts ############### 
tscripts <- map(list.files( here("data_large","tscripts_clean"),full.names = T),readRDS) 
names(tscripts) <- list.files( here("data_large","tscripts_clean"),full.names = F)
tscripts_df <- 
  data.table::rbindlist(tscripts,use.names = T,fill=T,idcol = "input_file") %>% 
  mutate(broadcast_date_fmt=as.Date(str_remove(input_file,".rds"))) %>% 
  left_join(meta_df,by=c("broadcast_date_fmt"),
            relationship="many-to-one",na_matches="never")
# naniar::vis_miss(tscripts_df,warn_large_data = F)

tscript_nest <- tidyr::nest(tscripts_df,.by=c('input_file','billed_duration','id',
                                             colnames(meta_df)))

tscript_nest$description_list <- as.list(NA)
tscript_nest$description_list <- map(tscript_nest$description, str_split, "\n")
tscript_nest$description_timestamp <- map(tscript_nest$description_list,
                                         ~str_subset(.x[[1]],"[:digit:]{1,2}:[:digit:]{2}")) %>% 
  map(.,~map_chr(.x,str_extract,"[:digit:]{1,2}:[:digit:]{2}"))
tscript_nest$description_title <- map(tscript_nest$description_list,
                                     ~str_subset(.x[[1]],"[:digit:]{1,2}:[:digit:]{2}")) %>% 
  map(~map_chr(.x,str_remove,"[:digit:]{1,2}:[:digit:]{2}")) %>% 
  map(trimws)

# convert timestamps to seconds
tscript_nest$description_timestamp_min <- map(tscript_nest$description_timestamp,
                                              str_extract, ".*(?=:)") %>% 
  map(., ~as.numeric(.x)*60)

tscript_nest$description_timestamp_sec <- map(tscript_nest$description_timestamp,
                                              str_extract, "(?<=:)[:digit:]{1,2}") %>% 
  map(as.numeric) %>% 
  map2(., tscript_nest$description_timestamp_min, ~.x+.y)

tscript_nest$data_present <- map_dbl(tscript_nest$data, nrow)


# tscript_df <- group_by(tscript_df,id) %>% mutate(error = ifelse(n()==1, 1, 0))
table(map_dbl(tscript_nest$description_timestamp,length))
rm(tscripts)


saveRDS(tscript_nest, "/Volumes/4tb_sam/ref_24/data_large/tscripts.rds")
############ sample frames #############

vids_to_sample <- list.files(here::here('data_large','clean_audiovisual'),full.names = T) %>% str_subset(".mp4$")



dir.create(here::here("data_large","frames"))
frame_sampler <- function(vid_name){
    av::av_video_images(vid_name,
                      destdir = paste(here::here("data_large","frames"),str_sub(vid_name,-14,-5),sep = "/"),
                      format = "jpg",
                      fps = .5) #extract images, fps of .5 = sampling rate = one frame every 2 seconds
}



parallel::mclapply(vids_to_sample, frame_sampler, mc.cores=5)

############### Crop and ocr title frames ############### 
frames_dir <- here("data_large","frames")
array_dir <- here("data_large","frame_arrays")
ocr_dir <- here("data_large","ocr_noblue")

vid_dirs_rel <- list.files(here("data_large","frames"),full.names = FALSE) # relative paths to frames

remove_blue_pixels <- function(img, blue_threshold = 200) {
  blue_channel <- image_channel(img, "blue")  # Separate the blue channel
  # create a binary mask based on the blue threshold
  blue_mask <- image_fx(blue_channel, expression = paste0("u < ", blue_threshold / 255, " ? 0 : 1"))
  # Convert blue areas to black by multiplying the mask with the original image
  img_modified <- image_composite(image_background(blue_mask, "black"), img, operator = "multiply")
  # Return the modified image
  return(img_modified)
}


image_array_func <- function(vid_dir_rel, frames_dir) { 
  
  engine_ger <- tesseract("deu", # language option for engine
                          options = list(tessedit_char_blacklist = "©®{}}@_:!=|™<>()[]—~\\")) # blacklist
  engine<-engine_ger
  # vid_dir_rel = relative path to video from frame directory (frames_dir) and array dir (array_dir)
  path <- list.files(paste(frames_dir, # load all frames from subdirectory of vid_dir_rel frames directory
                           vid_dir_rel,
                           sep = "/"),
                     full.names = TRUE)
  image_array_list <- lapply(path,
                             magick::image_read)  # load as array
  # get video format
  dims <- map(path,av::av_media_info) %>% map(pluck, "video") %>% map(.,~list("width"=.x$width,"height"=.x$height)) %>% unique() %>% unlist()
  
  if (length(dims)!=2) { # break if format not valid
    print("error")
    break
  }
  prop_width <- dims["width"] / 1280
  prop_height <- dims["height"] / 720
  
  # adjust dims for different formats of video in the sample
  top_crop_dims <- paste(paste(700*prop_width,40*prop_width,sep="x"),
                         70*prop_height,
                         415*prop_height,
                         sep="+")
  
  line1_crop_dims <- paste(paste(700*prop_width,70*prop_width,sep="x"),
                           70*prop_height,
                           455*prop_height,
                           sep="+")
  
  line2_crop_dims <- paste(paste(700*prop_width,70*prop_width,sep="x"),
                           70*prop_height,
                           520*prop_height,
                           sep="+")
  
  all_crop_dims <- paste(paste(700*prop_width,150*prop_width,sep="x"),
                         100*prop_height,
                         450*prop_height,
                         sep="+")
  
  
  # old pre-processing
  # out_top <-map(image_array_list, image_crop,top_crop_dims)  %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  # out_line1 <-map(image_array_list, image_crop,line1_crop_dims) %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  # out_line2 <- map(image_array_list, image_crop,line2_crop_dims) %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  # out_all <-map(image_array_list, image_crop,all_crop_dims)  %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  # new pre-processing
  out_top   <-map(image_array_list, image_crop,top_crop_dims)     %>% map(remove_blue_pixels)  %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  out_line1 <-map(image_array_list, image_crop,line1_crop_dims)   %>% map(remove_blue_pixels)  %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  out_line2 <-map(image_array_list, image_crop,line2_crop_dims)   %>% map(remove_blue_pixels)  %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  out_all   <-map(image_array_list, image_crop,all_crop_dims)     %>% map(remove_blue_pixels)  %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  
  
  print(pryr::mem_used()) # check size
  
  saveRDS(list(out_top=out_top,
               out_line1=out_line1,
               out_line2=out_line2,
               out_all=out_all), 
          file =here(ocr_dir,paste0(vid_dir_rel,"_ocr.rds"))) # save to target directory
  print(out_top[15:17])
}
# loop over all videos
# tic()
# for (i in seq_along(vid_dirs_rel)) {
#   print(i)
#   image_array_func(vid_dirs_rel[i] ,frames_dir = frames_dir)
# }
# toc()
# beepr::beep()

walk(vid_dirs_rel, image_array_func ,frames_dir = frames_dir)
tic()
mclapply(vid_dirs_rel, image_array_func ,frames_dir = frames_dir, mc.cores=5)
toc()
beepr::beep()


############### Process OCR and clip ############### 
raw_ocr_output_list <- map(list.files(here("data_large","ocr_noblue"),full.names = T),
                           readRDS)

names(raw_ocr_output_list) <- str_remove(list.files(here("data_large","ocr_noblue"),full.names = F),"_ocr.rds")

# skip one video where frame positioning doesn't align with all other ones (this one has a sign-language interpreter for some reason)
raw_ocr_output_list <- raw_ocr_output_list[list.files(here("data_large","ocr_noblue"))!="2019-04-11_ocr.rds"]

##### Confidence measures ########
# helper function
replace_nans <- function(df){ # all NaNs or <0 values in df -> 0
  df[is.na(df)|df<0] <- 0
  return(df)
}

# get out confidence measures
make_ocr_conf_df <- function(raw_ocr_output_list_element){ # list of ocr dfs associated w/  a vid, 4 per frame
  avg_conf_top <- raw_ocr_output_list_element %>% 
    pluck(1) %>% # choose topline ocr
    map_dbl(., ~mean(.x$confidence))# take mean of ocr confidence wrt all topline words
  
  avg_conf_line1 <- raw_ocr_output_list_element %>% 
    pluck(2) %>% # same, but w/ 1st line of topic text
    map_dbl(., ~mean(.x$confidence))
  
  avg_conf_line2 <- raw_ocr_output_list_element %>% 
    pluck(3) %>% # same but w/ 2nd line
    map_dbl(., ~mean(.x$confidence))
  
  avg_conf_all <- raw_ocr_output_list_element %>% 
    pluck(4) %>% # same, but with ocr of all text
    map_dbl(., ~mean(.x$confidence))
  
  # same as above four, but with maximum confidence values
  # will throw up warnings, can ignore
  max_conf_top <- raw_ocr_output_list_element %>% 
    pluck(1) %>% 
    map_dbl(., ~max(.x$confidence, na.rm=F))# get max vals
  
  max_conf_line1 <- raw_ocr_output_list_element %>% 
    pluck(2) %>% 
    map_dbl(., ~max(.x$confidence, na.rm=F))
  
  max_conf_line2 <- raw_ocr_output_list_element %>% 
    pluck(3) %>% 
    map_dbl(., ~max(.x$confidence, na.rm=F))
  
  max_conf_all <- raw_ocr_output_list_element %>% 
    pluck(4) %>% 
    map_dbl(., ~max(.x$confidence, na.rm=F))
  
  
  ocr_output_df <- dplyr::bind_cols(
    id=1:length(raw_ocr_output_list_element[[1]]), # unique id for each frame in a vid
    avg_conf_top=avg_conf_top,
    avg_conf_line1=avg_conf_line1,
    avg_conf_line2=avg_conf_line2,
    avg_conf_all=avg_conf_all,
    max_conf_top=max_conf_top,
    max_conf_line1=max_conf_line1,
    max_conf_line2=max_conf_line2,
    max_conf_all=max_conf_all
  )
  ocr_output_df[ocr_output_df<=0] <- 0 # deal w/ negatives created by max func
  return(ocr_output_df)
}

ocr_conf_dfs_list <- map(raw_ocr_output_list, # get confidence measures for all vids
                         make_ocr_conf_df) %>% 
  map(replace_nans) # all NaNs and -Inf output --> 0 (solves problem w/ max func assigning -Inf)

###### Find windows 
conf_threshold_windows <- function(ocr_conf_df, # input df of frames info
                                   window_lng = 2, # window of frames either side of frame to look at
                                   thresh = 90, # threshold
                                   num_greater = 3 # num. of obs > threshold in window for classification as cutpoint
){
  conf_thresh <- rep(0,window_lng) # initialize vec
  
  
  # loops over all frames in vid. If ocr confidence in *num_greater* of frames in a window of length *window_lng* is greather than a threshold *thresh*, then i=1, 0 o/w
  for(i in 5:(nrow(ocr_conf_df)-5)){ # loop over all frames in video, starting w/ 5th frame, ending w/ 5th to last (no chance that cutpoint will be in intro of vid)
    avg_conf_top_i <- ocr_conf_df$avg_conf_top[(i-window_lng):(i+window_lng)] # get conf vals for top lines of all frames in window
    avg_conf_line1_i <- ocr_conf_df$avg_conf_line1[(i-window_lng):(i+window_lng)] # same for topic line 1
    avg_conf_line2_i <- ocr_conf_df$avg_conf_line2[(i-window_lng):(i+window_lng)]
    avg_conf_all_i <- ocr_conf_df$avg_conf_all[(i-window_lng):(i+window_lng)]
    
    # if # of ocr'd frames w/ confidence level > threshold for any of the ocr'd lines is > *num_grater* param, then center frame in window is = 1
    conf_thresh[i] <- (sum(avg_conf_top_i > thresh, na.rm=T) > num_greater | # see how many frames in window have avg. top line confidence level > thresh
                         sum(avg_conf_line1_i > thresh, na.rm=T) > num_greater | 
                         sum(avg_conf_line2_i > thresh, na.rm=T) > num_greater |
                         sum(avg_conf_all_i > thresh, na.rm=T) > num_greater)
  }
  return(conf_thresh)
}

conf_thresh_90_list <- map(ocr_conf_dfs_list, # find thresholds for all videos
                           conf_threshold_windows,
                           window_lng=5,
                           thresh=75,
                           num_greater=2)

# take data frames of ocr output and the cuts based on thresholds from prev. func
# prev. func found window in which cut frames prob is
#this func tries to find exact start and end frames for cutpoints ($def_cut_seg_start,$def_cut_seg_end)
# also returns list of dataframes, each entry is ocr_conf_df for set of frames labeled as cutpoints
make_cuts <- function(conf_thresh_90,ocr_conf_df,avg_or_max){
  # gives indices of ends of vids and end of cutpoints segments
  runs <- accumulate(rle(conf_thresh_90)$lengths, sum) 
  
  cutpoint_segs <- sort( # start and end indices of cutpoint segments
    c(runs[rle(conf_thresh_90)$values==0] + 1,
      runs[rle(conf_thresh_90)$values==1]))
  
  if((length(cutpoint_segs) %% 2)==1){ # if odd
    cutpoint_segs <- cutpoint_segs[-length(cutpoint_segs)] # junk last element 
  }
  
  # list of first cut at cutpoint segments, each list contains start and endpoint
  cutpoint_segs <- transpose(
    list(cutpoint_segs[c(TRUE,FALSE)],
         cutpoint_segs[c(FALSE,TRUE)]))
  
  # list of ocr output dataframes, consider frames 3 to either side of the first frames that meet window criteria
  if(avg_or_max=="avg"){
    cuts <- cutpoint_segs %>% 
      map(., ~ocr_conf_df[ (.x[[1]] - 3):(.x[[2]] + 3), ]) %>% 
      map(~filter(.x,
                  avg_conf_top > 90 | avg_conf_line1 > 90 |
                    avg_conf_line2 > 90 | avg_conf_all > 90))
  }
  
  if(avg_or_max=="max"){
    cuts <- cutpoint_segs %>% 
      map(., ~ocr_conf_df[ (.x[[1]] - 3):(.x[[2]] + 3), ]) %>% # split df into segments w/ +/- 3 extra frames around windo
      map(~filter(.x,
                  max_conf_top > 90 | max_conf_line1 > 90 |
                    max_conf_line2 > 90 | max_conf_all > 90))
  }
  
  cut_seg_start <- map_dbl(cuts, ~min(.x$id))
  cut_seg_end <- map_dbl(cuts, ~max(.x$id))
  
  return(list(cuts=cuts,
              cut_seg_start=cut_seg_start,
              cut_seg_end=cut_seg_end))
}
definite_cuts <- map2(conf_thresh_90_list,
                      ocr_conf_dfs_list,
                      make_cuts,
                      avg_or_max="avg")


############### Tesst accuracy of clipping ############### 

# link cutopints w/ dates (metadata broadcast_date_fmt)
definite_cuts_cutpointpairs <- map(definite_cuts, ~.x[c("cut_seg_start","cut_seg_end")]) %>% 
  rbindlist(use.names=T,idcol="broadcast_date_fmt") %>% 
  as.data.frame() %>% 
  mutate(broadcast_date_fmt=as.Date(broadcast_date_fmt)) %>% 
  nest(.by='broadcast_date_fmt') %>% 
  mutate(data_cutpoints=data, .keep="unused")

tscript_nest_cut <- inner_join(tscript_nest, definite_cuts_cutpointpairs, 
                               by = "broadcast_date_fmt")
tscript_nest_cut <- tscript_nest_cut[map_dbl(tscript_nest_cut$description_timestamp_sec, length)>2,]
  

# initialize test vectors
mean_starts_est_error <- c()
prop_win_5_est <- c()
mean_starts_act_error <- c()
prop_win_5_act <- c()

for (i in 1:nrow(tscript_nest_cut)){
  
  starts_est <- tscript_nest_cut$data_cutpoints[[i]]$cut_seg_start 
  starts_act <- tscript_nest_cut$description_timestamp_sec[[i]] / 2
  starts_act <- starts_act[starts_act!=0] # remove start of video cutpoint
  # for each estimated cutpoint, take diff between estimated and nearest actual
  starts_est_error <- c()
  for (j in seq_along(starts_est)) {
    starts_est_error[j] <- min(abs(starts_est[j]-starts_act))
  }
  mean_starts_est_error[i] <- mean(starts_est_error)
  prop_win_5_est[i] <- sum(starts_est_error <= 15) / length(starts_est_error)
  
  # for each actual cutopint, take diff between actual and nearest estimated
  starts_act_error <- c()
  for (j in seq_along(starts_act)) {
    starts_act_error[j] <- min(abs(starts_act[j]-starts_est))
  }
  mean_starts_act_error[i] <- mean(starts_act_error)
  prop_win_5_act[i] <- sum(starts_act_error <= 15) / length(starts_act_error)
}

par(mfrow=c(2,2))
hist(mean_starts_est_error,breaks=20, xlim=c(0,50))
hist(prop_win_5_est, breaks=20, xlim=c(0,1), main = "Precision")
abline(v=mean(prop_win_5_est),col="red")
hist(mean_starts_act_error, breaks=20, xlim=c(0,50))
hist(prop_win_5_act, breaks=20, xlim=c(0,1), main = "Recall")
abline(v=mean(prop_win_5_act),col="red")

# re-run above to optimize



