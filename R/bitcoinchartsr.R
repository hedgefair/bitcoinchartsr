# library(stringr)
# library(quantmod)
# library(XML)
# library(RCurl)
# library(stringr)
# library(lubridate)

# =======================================================================================================
# Download daily full dump for specified symbol at:
# http://api.bitcoincharts.com/v1/csv/
# e.g. http://api.bitcoincharts.com/v1/csv/mtgoxUSD
# =======================================================================================================
download_daily_dump <- function(symbol, data.directory=system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), overwrite=FALSE) {
  full.path <- paste(data.directory, '/', symbol, '-dump.csv', sep='')
  if(file.exists(full.path)) {
    if(!overwrite) {
      message(paste(full.path, 'already exists, skipping download...'))
      return(full.path)
    } else {
      message(paste('Deleting old dump file', full.path))
      unlink(full.path)
    }
  }
  url <- paste('http://api.bitcoincharts.com/v1/csv/', symbol, '.csv', sep='')
  download.file(url, destfile=full.path, method='auto', quiet=FALSE, mode="w", cacheOK=TRUE, extra=getOption("download.file.extra"))
  return(full.path)
}

# =======================================================================================================
# Download daily full dump for all available symbols at:
# http://api.bitcoincharts.com/v1/csv/
# e.g. http://api.bitcoincharts.com/v1/csv/mtgoxUSD
# base.dir = base data directory
# overwrite = whether or not to overwrite existing dumps
# =======================================================================================================
download_all_daily_dumps <- function(base.dir=system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), overwrite=FALSE) {
  tryCatch(expr={
    all.symbols <- get_symbol_listing()
    sapply(all.symbols, function(x) {
      # if data dir doesn't exist create it
      dir.name <- paste(base.dir, x, sep='/')
      if(!file.exists(dir.name)) {
        message(paste('Creating data dir', dir.name))
        dir.create(dir.name, showWarnings=TRUE)
      }
      tryCatch(expr={
        file.name <- download_daily_dump(symbol=x, data.directory=dir.name, overwrite=overwrite)
        message(paste('Downloaded', file.name, '...'))
      }, error=function(e) {
        message(paste('Failed to download data for symbol:', x))
        message(paste('Error:', e))
      })
    })
  }, error=function(e) {
    message(paste('ABORTING! Failed to get symbol listing because of error:', e))
  })
}

# =======================================================================================================
# Get list of all currently available symbols from http://bitcoincharts.com/markets
# =======================================================================================================
get_symbol_listing <- function(markets.url='http://bitcoincharts.com/markets/', local.data.file=paste(system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), 'data/bitcoincharts-markets.csv', sep='/')) {
  message('Getting updated symbol list from bitcoincharts.com...')
  txt <- NA
  tryCatch(expr={
    txt <- getURL(markets.url)
  }, error=function(e) {
    # if online lookup fails go to local file
    stop(paste('Failed to consult online listing, error:', e))
#     all.symbols <- read.csv(file=local.data.file, sep=',', header=TRUE)
#     return(all.symbols$symbol)
  })
  xmltext <- htmlParse(txt, asText=TRUE)
  xmltable <- xpathApply(xmltext, "//table//tr//td//nobr//a")
  all.symbols <- sort(unname(unlist(lapply(xmltable, FUN=function(x) { str_replace(str_replace(xmlAttrs(x)['href'], pattern='/markets/', replacement=''), pattern='\\.html', replacement='') }))))
  return(all.symbols)
}

# =======================================================================================================
# Get detailed listing of all currently available exchanges
# =======================================================================================================
get_exchange_info <- function(markets.url = 'http://bitcoincharts.com/markets/') {
  #   txt <- getURL(markets.url)
  #   xmltext <- htmlParse(txt, asText=TRUE)
  #   browser()
  #   xmltable <- xpathApply(xmltext, "//table//tr//td")
  #   
  #   all.symbols <- sort(unname(unlist(lapply(xmltable, FUN=function(x) { xmlValue(x) }))))
  #   all.symbols
  stop('Not implemented!')
}

# =======================================================================================================
# Utility function which returns the last trade in the most recent daily dump file
# =======================================================================================================
get_most_recent_trade <- function(symbol, data.directory=paste(system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), symbol, sep='/')) {
  if(!file.exists(data.directory)) stop('Data directory is missing!') # we should just download the files instead of punking out here...
  f <- list.files(path=data.directory, pattern='-[0-9]{1,}\\.csv$', full.names=TRUE)
  f <- sort(f)
  new.rows <- read.csv(f[ length(f) ], header=FALSE, sep=',', colClasses=c('numeric','numeric','numeric'), col.names=c('timestamp','price','amount'))   
  return(new.rows[ nrow(new.rows),  ])
}

# =======================================================================================================
# Download all historical trade data for a given symbol from bitcoincharts.com 
# and split into 20000-line csv files
# =======================================================================================================
prepare_historical_data <- function(symbol, data.directory=system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), download.daily.dump) {
  # delete the lock file
  unlink(paste(data.directory, 'done.lck', sep='/'), force=TRUE)
  # first delete all existing data in the directory
#   message(paste("Removing old processed data in directory (leaving dump file intact)", data.directory))
  f <- list.files(path=data.directory, full.names=TRUE, pattern='[0-9]{1,}\\.csv$') # get rid of processed files only
  sapply(f, FUN=function(x) { 
#     message(paste('Deleting', x))
    unlink(x) 
  })
  # now download the huge daily dump file
  dump.file <- ''
  if(download.daily.dump) {
    dump.file <- download_daily_dump(symbol=symbol, data.directory=data.directory)  
  } else {
    dump.file <- paste(data.directory, '/', symbol, '-dump.csv', sep='')
    if(!file.exists(dump.file)) {
      # file is missing download it!
      dump.file <- download_daily_dump(symbol=symbol, data.directory=data.directory)  
    }
  }
  # now let's process our dump file
  # split it into chunks of 20000 trades
  system(command=paste('split -l 20000 -a 5 -d ', dump.file, ' ', data.directory, '/', symbol, '-', sep=''), intern=TRUE)
  # rename all to .csv
  f <- list.files(path=data.directory, pattern=paste(symbol, '-[0-9]{5}$',sep=''), full.names=TRUE)
  sapply(f, FUN=function(x) { 
    last.date <- unlist(strsplit(system(command=paste('tail -n 1 ', x), intern=TRUE, ignore.stderr=TRUE),split=','))[1]
    first.date <- unlist(strsplit(system(command=paste('head -n 1 ', x), intern=TRUE, ignore.stderr=TRUE),split=','))[1]
    x.new <- gsub(pattern='[0-9]{5}', replacement=paste(first.date, last.date, sep='-'), x=x)
    x.new <- paste(x.new, 'csv', sep='.')
    file.rename(x, x.new) 
  })
  # now we will rebalance the ticks so we do not have the same timestamp spanning more than one file
  f <- list.files(path=data.directory, pattern='\\.csv$', full.names=TRUE)
  results<- sapply(f, FUN=function(x) { 
    # we put the try here because files in the original list may no longer exist after shuffling ticks and renaming
    try(silent=TRUE, expr={
      last.date <- unlist(strsplit(system(command=paste('tail -n 1 ', x), intern=TRUE, ignore.stderr=TRUE),split=','))[1]
      if(length(grep(last.date, x=f)) > 1) {
        # last.date of one file is the start.date of another, need to shuffle
        files <- sort(grep(last.date, x=f, value=TRUE))
#         message('=========================================================\n')
#         message(paste('Redistributing ticks in:', files))
        # now grab the first couple of lines of the newer file and drop them in the older file
        data <- read.csv(files[2], header=FALSE)
        move <- data[ (data$V1 == last.date), ]
#         message('Moving chunk:')
#         message(move)
#         message(paste('from', files[2], 'to', files[1]))
        write.table(x=move, file=files[1], append=TRUE, sep=',', row.names=FALSE, col.names=FALSE)
        # now fix the newer file
#         message(paste('Removing old file', files[2]))
        unlink(files[2], force=TRUE)
        move <- data[ (data$V1 > last.date), ]
        first.date <- move[1, 1]
        last.date <- move[nrow(move), 1]
        x.new <- paste(symbol, first.date, last.date, sep='-')
        x.new <- paste(x.new, '.csv', sep='')
#         message(paste('Creating new file', paste(data.directory, x.new, sep='/')))
        write.table(x=move, file=paste(data.directory, x.new, sep='/'), append=FALSE, sep=',', row.names=FALSE, col.names=FALSE)
#         message('=========================================================')
      }  
    })
  })
  file.create(paste(data.directory, 'done.lck', sep='/'), showWarnings=TRUE)
  dump.file
}

# =======================================================================================================
# get the most recent data
# http://api.bitcoincharts.com/v1/trades.csv?symbol=SYMBOL[&end=UNIXTIME]
# =======================================================================================================
get_ticker <- function(symbol, end = '', data.director=ysystem.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr')) {
  file.name <- paste(symbol, 'XXXXXXXXXX', sep='-')
  if(end != '') {
    file.name <- paste(file.name, as.character(end), sep='-')  
  } else {
    file.name <- paste(file.name, 'YYYYYYYYYY', sep='-')
  }
  file.name <- paste(file.name, 'csv', sep='.')
  ColClasses = c('numeric','numeric','numeric')
  url <- paste('http://api.bitcoincharts.com/v1/trades.csv?symbol=', symbol, sep='')
  if(end != '') {
    url <- paste(url, '&end=', end, sep='')  
  }
  full.path <- paste(data.directory,file.name,sep='/')
  download.file(url, destfile=full.path, method='auto', quiet = FALSE, mode = "w", cacheOK = TRUE, extra = getOption("download.file.extra"))
  # now we need to find the last timestamp downloaded to complete the file name
  last.line <- system(command=paste('tail -n 1 ', full.path), intern=TRUE, ignore.stderr=TRUE)
  first.line <- system(command=paste('head -n 1 ', full.path), intern=TRUE, ignore.stderr=TRUE)
  earliest.timestamp <- unlist(str_split(last.line,pattern=','))[1]
  latest.timestamp <- unlist(str_split(first.line,pattern=','))[1]
  file.name <- gsub(file.name,pattern='XXXXXXXXXX',replacement=earliest.timestamp)
  file.name <- gsub(file.name,pattern='YYYYYYYYYY',replacement=latest.timestamp)
  new.full.path <- paste(data.directory,file.name,sep='/')
  file.rename(full.path, new.full.path)
  return(earliest.timestamp)
}

# =======================================================================================================
# Download all historical trade data for a given symbol from bitcoincharts.com 
# and split into 20000-line csv files. Then parse the files and return tick data for the specified
# time period
# =======================================================================================================
get_trade_data <- function(symbol, start.date, end.date='', data.directory=paste(system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), symbol, sep='/')) {
  start.ts <- as.integer(as.POSIXct(as.Date(start.date), tz='GMT', origin='1970-01-01'))
  # find start file index
  f <- sort(list.files(path=data.directory, pattern='[0-9]{1,}\\.csv$', full.names=FALSE))
  start.idx <- 1
  end.idx <- length(f)
  for(i in 1:length(f)) {
    if(start.ts <= unlist(strsplit(f[i], split='-'))[2] | start.ts <= str_replace(unlist(strsplit(f[i], split='-'))[3], pattern='\\.csv', replacement='')) {
      start.idx <- i - 1 
      break
    }
  }
  # find the end file index
  if(end.date != '') {
    end.ts <- as.integer(as.POSIXct(as.Date(end.date), tz='GMT', origin='1970-01-01'))  
    for(i in 1:length(f)) {
      if(end.ts <= str_replace(unlist(strsplit(f[i], split='-'))[3], pattern='\\.csv', replacement='')) {
        end.idx <- i
        break
      }
    }  
  } else {
    # no end date specified so take most recent file
    end.idx <- length(f)
  }
#   message("Will gather tick data from the following files:")
  f <- sort(list.files(path=data.directory, pattern='[0-9]{1,}\\.csv$', full.names=TRUE))
#   message(f[ start.idx:end.idx ])
  tickdata <- data.frame(timestamp=0,price=0,amount=0)
  for(fl in f[ start.idx:end.idx  ]) {
#     message(paste('Getting data from',fl))
    new.rows <- ''
    tryCatch( expr= { 
      new.rows <- read.csv(fl, header=FALSE, sep=',', colClasses=c('numeric','numeric','numeric'), col.names=c('timestamp','price','amount')) 
    }, warning=function(w) { 
      message(paste('WARNING:', w$message, sep=' '))
      # this file needs newline char at end of file, add one and try to read it again
      if(grep(w$message,pattern='incomplete final line found')) {
        cat('\n', file=fl, sep='', append=TRUE)
        read.csv(fl, header=FALSE, sep=',', colClasses=c('numeric','numeric','numeric'), col.names=c('timestamp','price','amount')) 
      }
      message('File fixed')
    }, error=function(e) {
      message(paste('ERROR:',e$message,sep=' ')) 
    })
    tickdata <- rbind(tickdata,new.rows)
  }
  # skip the first row it was just dummy data
  tickdata <- tickdata[2:nrow(tickdata),]
  # now filter for date range from start
  tickdata <- tickdata[ tickdata$timestamp >= start.ts, ]
  # ok now if no end.date is specified we need to work our way backwards to the last available data
  if(end.date == '') {
    end.ts <- as.integer(as.POSIXct(Sys.time() + days(1), tz='GMT', origin='1970-01-01'))  
  } else {
    if(as.integer(tickdata[ nrow(tickdata), 'timestamp']) > end.ts) {
      # required data is already present, now filter out stuff that exceeds specified end.date
      tickdata <- tickdata[ tickdata$timestamp <= end.ts, ]  
    } else {
      # we need to work backwards until we get the required data 
      earliest.timestamp <- end.ts - 1
      while(earliest.timestamp > end.ts) {
        earliest.timestamp <- get_ticker(symbol=symbol, end=earliest.timestamp, data.directory=data.directory)   
      }
    }
  }
  return(tickdata)
}

# ==============================================================================================
# function which converts raw tick data to xts onject with OHLC
# possible formats include: "%Y%m%d %H %M %S" (seconds), 
# "%Y%m%d %H %M" (minutes), "%Y%m%d %H" (hours), "%Y%m%d" (daily)
# ==============================================================================================
to.ohlc.xts <- function(ttime, tprice, tvolume, fmt) {
  ttime.int <- format(ttime,fmt)
  df <- data.frame(time = ttime[tapply(1:length(ttime),ttime.int,function(x) {head(x,1)})],
                   Open = tapply(tprice,ttime.int,function(x) {head(x,1)}), 
                   High = tapply(as.numeric(tprice),ttime.int,max),
                   Low = tapply(as.numeric(tprice),ttime.int,min),
                   Close = tapply(tprice,ttime.int,function(x) {tail(x,1)}),
                   Volume = tapply(as.numeric(tvolume),ttime.int,function(x) {sum(x)}),
                   Ticks = tapply(as.numeric(tvolume),ttime.int, length))
  # fill in any missing time slots and align along an appropriate boundary (minute, hour or day)
  if(fmt == '%Y%m%d %H %M %S') {
    ohlc.xts <- align.time(as.xts(df[2:7], order.by=df$time), n=1)  
    idx <- align.time(seq(start(ohlc.xts), end(ohlc.xts), by=1), n=1)
  } else if(fmt == '%Y%m%d %H %M') {
    ohlc.xts <- align.time(as.xts(df[2:7], order.by=df$time), n=60)  
    idx <- align.time(seq(start(ohlc.xts), end(ohlc.xts), by=60), n=60)
  } else if(fmt == '%Y%m%d %H') {
    ohlc.xts <- align.time(as.xts(df[2:7], order.by=df$time), n=60*60) 
    idx <- align.time(seq(start(ohlc.xts), end(ohlc.xts), by=60*60), n=60*60)
  } else if(fmt == '%Y%m%d') {
    ohlc.xts <- align.time(as.xts(df[2:7], order.by=df$time), n=60*60*24) 
    idx <- align.time(seq(start(ohlc.xts), end(ohlc.xts), by=60*60*24), n=60*60*24)
  } else if(fmt == '%Y%m') {
    ohlc.xts <- as.xts(df[2:7], order.by=df$time)
    return(ohlc.xts)
  } else if(fmt == '%Y') {
    ohlc.xts <- as.xts(df[2:7], order.by=df$time)
    return(ohlc.xts)
  } 
  empties <- data.frame(Open = rep(NA, times=length(idx)),
                        High = rep(NA, times=length(idx)),
                        Low = rep(NA, times=length(idx)),
                        Close = rep(NA, times=length(idx)),
                        Volume = rep(NA, times=length(idx)),
                        Ticks = rep(NA, times=length(idx)),
                        row.names=idx)
  empties <- as.xts(empties, order.by=idx)
  ohlc.xts <- merge(ohlc.xts, empties)[,1:6]
  # fill any NA values with previous available ticks close ( vol = 0, op, hi, lo all should equal previous close)
  missing.vals <- which(is.na(ohlc.xts$Open))
  ohlc.xts[ is.na(ohlc.xts$Volume), 'Volume' ] <- 0
  ohlc.xts[ is.na(ohlc.xts$Ticks), 'Ticks' ] <- 0
  ohlc.xts[ ,'Close' ] <- na.locf(ohlc.xts$Close)
  ohlc.xts[ missing.vals, c('Open', 'High', 'Low')] <- ohlc.xts[ missing.vals, 'Close']
  ohlc.xts[1:(nrow(ohlc.xts) - 1), ]
}

# ==============================================================================================
# Call this method to get tick data for exchange on bitcoincharts.com
# returns xts object for symbol for specified time frame
# symbol = virtexCAD, mtgoxUSD, mtgoxCAD, etc
# start.date, end.date = data date range in YYYY-MM-DD format NOTE: this MUST be in character format!!!!
# data.directory = location of data files
# ohlc.frequency = minutes, hourly, daily, monthly, seconds
# download.data = whether or not to perform a fresh download of daily data dump file
# ==============================================================================================
get_bitcoincharts_data <- function(symbol, start.date=as.character((Sys.Date() - months(1))), end.date=as.character(Sys.Date() + days(1)), ohlc.frequency = 'hours', data.directory=paste(system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), symbol, sep='/'), download.data=FALSE, auto.assign=FALSE, environment=.GlobalEnv) {
  call <- match.call()
  if(!(ohlc.frequency %in% c('seconds', 'minutes', 'hours', 'days', 'months', 'years'))) {
    stop("OHLC frequency must be one of following: seconds, minutes, hours, days, months, years")
  }
  if(!(str_detect(start.date, pattern='[0-9]{4}-[0-9]{2}-[0-9]{2}'))) {
    stop("start.date must be in YYYY-MM-DD format")
  }
  if(!(str_detect(end.date, pattern='[0-9]{4}-[0-9]{2}-[0-9]{2}'))) {
    stop("end.date must be in YYYY-MM-DD format")
  }
#   message(paste('Preparing historical data for: ', symbol, sep=''))
  #   browser()
  #   if(file.exists(paste(data.directory, 'done.lck', sep='/'))) {
  #     if(download.data) {
  #       prepare_historical_data(symbol=symbol, data.directory=data.directory, download.daily.dump=download.data)       
  #     }
  #     message('Data already prepared')
  #   } else {
  prepare_historical_data(symbol=symbol, data.directory=data.directory, download.daily.dump=download.data)       
  #   }
  message(paste('Getting tick data for: ', symbol, sep=''))
  tickdata <- get_trade_data(symbol=symbol, data.directory=data.directory, start.date=start.date, end.date=end.date)
  if(ohlc.frequency == 'seconds') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='GMT',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H %M %S")  
  } else if(ohlc.frequency == 'minutes') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='GMT',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H %M")
  } else if(ohlc.frequency == 'hours') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='GMT',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H")  
  } else if(ohlc.frequency == 'days') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='GMT',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d")  
  } else if(ohlc.frequency == 'months') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='GMT',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m")  
  } else if(ohlc.frequency == 'years') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='GMT',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y")  
  }
  colnames(ohlc.data.xts) <- c('Open', 'High', 'Low', 'Close', 'Volume', 'Ticks')
  if(!auto.assign) {
    # return to caller
    return(ohlc.data.xts)  
  } else {
    # assign it a-la quantmod
    assign(x=symbol, value=ohlc.data.xts, envir=environment)
    return(NA)
  }
}

# ==============================================================================================
# load data for all known symbols into the global environment
# data.base.dir = base directory for tick data files
# start.date, end.date = 'YYYY-MM-DD' (has to be CHARACTER!!!)
# ohlc.frequency = minutes, hourly, daily, monthly
# download.data = whether or not to re-download data files
# defaults will result in one month worth of daily data being pulled for all exchanges
# ==============================================================================================
load_all_data <- function(data.base.dir=system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), start.date=as.character(Sys.Date() - months(1)), end.date=as.character(Sys.Date() + days(1)), ohlc.frequency='daily', download.data=FALSE) {
  all.symbols <- get_symbol_listing()
  dev.null <- sapply(all.symbols, function(x) {
    message(paste('Loading', x, '...'))
    tryCatch(expr={
      get_bitcoincharts_data(symbol=x, data.directory=paste(data.base.dir, x, sep='/'), start.date=start.date, end.date=end.date, ohlc.frequency=ohlc.frequency, download.data=download.data, auto.assign=TRUE)
    }, error=function(e) {
      message(paste('Failed to load data for', x, 'error:', e))
    })
  })
}