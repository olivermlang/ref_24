pacman::p_load(data.table,lubridate,here,stringr,dplyr,readr,jsonlite,purrr,av,googleCloudStorageR,here)
tages_vids <- list.files("/Volumes/4tb_sam/tages/",full.names = T)

vidoutput <- tages_vids |> 
  stringr::str_subset(".json$|.wav$|.webm$|.mkv$|,mp4$")
all_json_names <- tages_vids |> 
  str_subset(".info.json")

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
                                   "upload_date",
                                   "resolution","fps","width","height",
                                   "dynamic_range","vcodec","aspect_ratio",
                                   "audio_channels",
                                   "playlist","playlist_count","playlist_id",
                                   "playlist_title","n_entries"))) %>% as.data.frame()


meta_df$broadcast_date <- str_extract(meta_df$title, "[:digit:]{1,2}.[:digit:]{1,2}.[:digit:]{2,4}")
meta_df$broadcast_date  <- gsub("(\\d{1,2}\\.\\d{1,2}\\.)(\\d{2})$", "\\120\\2", meta_df$broadcast_date )
meta_df$broadcast_date_fmt <- as.Date(meta_df$broadcast_date, format = "%d.%m.%Y")
broadcast_date_fmt <- meta_df$broadcast_date_fmt[!is.na(meta_df$broadcast_date_fmt)&
                                                   meta_df$broadcast_date_fmt<=max(date_seq)&
                                                   meta_df$broadcast_date_fmt>=min(date_seq)]


broadcast_date <- meta_df$broadcast_date[!is.na(meta_df$broadcast_date)]
btages_remedial <- list.files("/Volumes/4tb_sam/tages_remedial/")%>% str_subset(".wav") %>% 
  as.Date(format="%d.%m.%Y")
#have videos for every date in sequence
length(date_seq[!date_seq%in%c(broadcast_date_fmt,btages_remedial)])==0
length(broadcast_date_fmt)==length(date_seq)
intersect(broadcast_date_fmt,btages_remedial)
which(duplicated(broadcast_date_fmt))
length(c(broadcast_date_fmt,btages_remedial))==length(date_seq)

all_vids <- c(btages_remedial,broadcast_date_fmt)
meta_df_totscribe <- filter(meta_df,
                            !is.na(broadcast_date_fmt))





tages_remedial_fp <- list.files("/Volumes/4tb_sam/tages_remedial/",full.names =T)


bucket <- gcs_get_global_bucket()
bucket_objs <- gcs_list_objects(bucket = bucket,
                                delimiter=FALSE,
                                detail = c("summary", "more", "full"),
                                prefix = NULL) 

tages_obj <- str_subset(bucket_objs$name,"tagesschau.*wav")
tages_rem <- str_subset(bucket_objs$name,"tages_remedial.*wav")
# virtual env w/ python3.12 
venv_path <- "~/Documents/censorship/tv/data/transcription/python_stuff/transcribe-vids/bin/activate"
venv_activiation    <- paste0('source ',venv_path)
# add arguments to script
python_tscribe_base <- 'python3 TSCRIBEPYPATH FULLPATH JSONBASEDIR CHANNELPROGRAMID'
broadcast_date_1 <- date_seq[i]


tscribe_tag <- function(broadcast_date_1){
  
  
  
  dateseq_int <- format(broadcast_date_1,"%Y%m%d")
  dateseq_pt  <- format(broadcast_date_1, "%d.%m.%Y")
  
  
  
  if (any(str_detect(list.files(here("large_data","tscripts")),
                                paste(dateseq_int,dateseq_pt,
                                      collapse="|")))) {
    print(paste0(broadcast_date_1," -- json already present"))
    next
  }
  if(any(str_detect(tages_rem, dateseq_pt))){
    full_path <- str_subset(tages_rem, dateseq_pt)
    print(full_path)
    
    python_tscribe_correct <- python_tscribe_base %>% 
      str_replace("FULLPATH",        full_path) %>% 
      str_replace("JSONBASEDIR",    here("data_large","tscripts") ) %>% 
      str_replace("TSCRIBEPYPATH",  here("scripts","tages_tscribe.py")) %>% 
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
        str_replace("JSONBASEDIR",    here("data_large","tscripts") ) %>% 
        str_replace("TSCRIBEPYPATH",  here("scripts","tages_tscribe.py")) %>% 
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
    }
  }
  
  json_input <- here("data_large","tscripts",str_replace(full_path,".wav",".transcript.json")) %>% 
    str_remove("tagesschau|tages_remedial") %>% jsonlite::read_json()
  
  input_file <- names(json_input$results)
  billed_duration <- json_input$totalBilledDuration
  
  if (billed_duration=="0s") {
    tscript_df <- "error"
  } else {
    results      <- json_input$results[[1]]
    tscripts_all <- results$transcript[[1]]
    
    result_endtiming    <- tail(map_chr(tscripts_all, pluck, "resultEndOffset"),1)
    result_langcode     <- map(tscripts_all, pluck, "languageCode")
    result_chanltag     <- map(tscripts_all, pluck, "channelTag")
    alternatives        <- map(tscripts_all, pluck, "alternatives")
    alternatives_length <- map_dbl(alternatives, length)
    
    
    wrd_list <- list()
    for (j in seq_along(alternatives)) {
      # if there are any words in this section of video
      if (length(alternatives[[j]])>0) {
        if (length(alternatives[[j]][[1]]$words)>0){
          wrd_list[[j]] <- alternatives[[j]][[1]]$words %>% 
            map(as.data.frame) %>% 
            data.table::rbindlist(use.names=T,fill=T) %>% 
            as.data.frame()
          wrd_list[[j]]$lang_code <- result_langcode[[j]]
          wrd_list[[j]]$chanltag <- result_chanltag[[j]]
        } else {
          next
        } 
      } else {
        next
      }
      
    }
    
    tscript_df <- data.table::rbindlist(wrd_list, use.names=T, fill=T) %>% as.data.frame()
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
    saveRDS(tscript_df, here("data_large","tscripts_clean",paste0(broadcast_date_1,".rds")))
  print(i)
}

for(i in 2:10){
  tscribe_tag(date_seq[i])
}

  ###############################################################################################
  # then extract json data and save off rds with output
  ###############################################################################################
  json_input <- jsonlite::read_json(here("tv","data","transcription","jsons",channel_program_id,str_replace(only_filename,".wav",".transcript.json")))
  
  input_file <- names(json_input$results)
  billed_duration <- json_input$totalBilledDuration
  
  if (billed_duration=="0s") {
    tscript_df <- "error"
  } else {
    results      <- json_input$results[[1]]
    tscripts_all <- results$transcript[[1]]
    
    result_endtiming    <- tail(map_chr(tscripts_all, pluck, "resultEndOffset"),1)
    result_langcode     <- map(tscripts_all, pluck, "languageCode")
    result_chanltag     <- map(tscripts_all, pluck, "channelTag")
    alternatives        <- map(tscripts_all, pluck, "alternatives")
    alternatives_length <- map_dbl(alternatives, length)
    
    
    wrd_list <- list()
    for (j in seq_along(alternatives)) {
      # if there are any words in this section of video
      if (length(alternatives[[j]])>0) {
        if (length(alternatives[[j]][[1]]$words)>0){
          wrd_list[[j]] <- alternatives[[j]][[1]]$words %>% 
            map(as.data.frame) %>% 
            data.table::rbindlist(use.names=T,fill=T) %>% 
            as.data.frame()
          wrd_list[[j]]$lang_code <- result_langcode[[j]]
          wrd_list[[j]]$chanltag <- result_chanltag[[j]]
        } else {
          next
        } 
      } else {
        next
      }
      
    }
    
    tscript_df <- data.table::rbindlist(wrd_list, use.names=T, fill=T) %>% as.data.frame()
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
  }
  saveRDS(tscript_df,df_path)
  
  ###############################################################################################
  # check output for errors: if error, flag
  ###############################################################################################
  test_outcome <- FALSE
  
  if ( nrow(tscript_df)>1 ) {
    test_outcome <- 
      # check that there is data in the output transcriptions
      tscript_df$billed_duration[1] > 0
    # check that the names of the input data from gcloud and the json output name line up
    str_remove(str_remove(unique(tscript_df$input_file), paste0(channel_program_id,"/")), "gs://tscribe_audio/")==only_filename
    test_result <- "NA"
    # save off results of above tests
    if (test_outcome) {
      test_result <- list(no_nonempty_rows=sum(!is.na(tscript_df$word)),name_inputgcloud=unique(tscript_df$input_file),name_inputlocal=only_filename)
    }
  } else {
    test_result <- "no transcription output"
  }
  saveRDS(list(test_outcome=test_outcome,test_result=test_result),test_path)
  
  ###############################################################################################
  # if no errors, delete object from google cloud
  ###############################################################################################
  if (all(test_result=="error")) {
    print(paste0(only_filename," --- end of script test failed"))
    next
  } 
  
  print(paste(c(head(tscript_df$word, 10)," | ",tail(tscript_df$word, 10)), collapse=" "))
  toc()
  print(paste0(i,": Finished"))
  
  
}



x <- as.Date(broadcast_date, "%d.%m.%Y") 
x <- x[!is.na(x)]
x_seq <- seq(min(x),max(x),by=1)
x[!x%in%]
date_seq[!date_seq%in%x]
#hello 
meta_df$broadcast_date[meta_df$id=="08qssBDxfUU"] <- "20190102"
meta_df$broadcast_date[meta_df$id=="4UQvsM6XFbo"] <- "20190510"
meta_df$broadcast_date[meta_df$id=="yWgQW47vOVg"] <- "20191011"

yWgQW47vOVg
# to delete
todelete <- c("WKVxVK3sBC8","hUur3brG7LE","D7FfAlYRsBk")