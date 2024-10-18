pacman::p_load(data.table,lubridate,here,stringr,dplyr,readr,jsonlite,purrr,av,tesseract,
               googleCloudStorageR,magick,parallel,janitor)
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


parallel::mclapply(date_seq[1:length(date_seq)],tscribe_tag,mc.cores=10)


for(i in 1:5){
  print(i)
  tscribe_tag(date_seq[i])
}


for(i in 16:32){
  tscribe_tag(date_seq[i])
}


tscripts <- map(list.files( here("data_large","tscripts_clean"),full.names = T),readRDS) 
names(tscripts) <- list.files( here("data_large","tscripts_clean"),full.names = F)
tscripts_df <- 
  data.table::rbindlist(tscripts,use.names = T,fill=T,idcol = "input_file") %>% 
  mutate(broadcast_date_fmt=as.Date(str_remove(input_file,".rds"))) %>% 
  left_join(meta_df,by=c("broadcast_date_fmt"),
            relationship="many-to-one",na_matches="never")
naniar::vis_miss(tscripts_df,warn_large_data = F)

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
# tscript_df <- group_by(tscript_df,id) %>% mutate(error = ifelse(n()==1, 1, 0))
table(map_dbl(tscript_nest$description_timestamp,length))

summary(tscript_df$duration[tscript_df$error==1])
summary(tscript_df$duration[tscript_df$error==0])

table(tscript_df$vcodec[tscript_df$error==1])
table(tscript_df$vcodec[tscript_df$error==0])


summary(tscript_df$anyaudio[tscript_df$error==1])
summary(tscript_df$anyaudio[tscript_df$error==0])


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


frames_dir <- here("data_large","frames")
array_dir <- here("data_large","frame_arrays")
ocr_dir <- here("data_large","ocr")

vid_dirs_rel <- list.files(here("data_large","frames"),full.names = FALSE) # relative paths to frames

image_array_func <- function(vid_dir_rel, frames_dir) { 
  
  engine_ger <- tesseract("deu", # language option for engine
                          options = list(tessedit_char_blacklist = "©®{}}@_:!=|™<>()[]—~\\")) # blacklist
  engine<-engine_ger
  # vid_dir_rel = relative path to video from frame directory (frames_dir) and array dir (array_dir)
  image_array_list <- lapply(list.files(paste(frames_dir, # load all frames from subdirectory of vid_dir_rel frames directory
                                              vid_dir_rel,
                                              sep = "/"),
                                        full.names = TRUE),
                             magick::image_read)  # load as array

  out_top <-map(image_array_list, image_crop,"700x40+70+415")  %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  out_line1 <-map(image_array_list, image_crop,"700x70+70+455")  %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  out_line2 <- map(image_array_list, image_crop,"700x70+70+520") %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
  out_all <-map(image_array_list, image_crop,"700x150+100+450")  %>% purrr::map(magick::image_convert,type = 'Grayscale') %>% purrr::map(tesseract::ocr_data,engine=engine)
    
  print(pryr::mem_used()) # check size
  
  saveRDS(list(out_top=out_top,
               out_line1=out_line1,
               out_line2=out_line2,
               out_all=out_all), 
          file =here(ocr_dir,paste0(vid_dir_rel,"_ocr.rds"))) # save to target directory
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
mclapply(vid_dirs_rel, image_array_func ,frames_dir = frames_dir, mc.cores=8)
toc()
beepr::beep()

z <- image_read(list.files(paste(frames_dir, # load all frames from subdirectory of vid_dir_rel frames directory
                                 vid_dir_rel,
                                 sep = "/"),
                           full.names = TRUE)[12])
x <- map(image_array_list, image_crop,"500x150+100+450") 
for(i in seq_along(x)){
  Sys.sleep(.2)
  plot(x[[i]])
}
plot(image_crop(z, "500x40+70+415")) # top
plot(image_crop(z, "500x150+100+450")) # main
plot(image_crop(z, "500x70+70+455")) # main top 
plot(image_crop(z, "500x70+70+520")) # main bottom
