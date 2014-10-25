# library(quantmod)
# library(XML)
# library(RCurl)
# library(stringr)
# library(lubridate)
# library(R.utils)
# library(data.table)

# ----------------------------------------------------------------------------------------------
# INTERNAL (PRIVATE) FUNCTIONS
# ----------------------------------------------------------------------------------------------

#' @title prepare_historical_data
#' @description Download all historical trade data for a given symbol from bitcoincharts.com and 
#' split into 20000-line csv files
#' @param symbol
#' @param data.directory
#' @param download.daily.dump
#' @param overwrite
#' @param debug
prepare_historical_data <- function(symbol, 
                                    data.directory, 
                                    download.daily.dump, 
                                    overwrite,
                                    debug) 
{
  # first delete all existing data in the directory
  if(debug) message(paste("Removing old processed data in directory (leaving dump file intact)", data.directory))
  f <- list.files(path=data.directory, full.names=TRUE, pattern='[0-9]{1,}\\.csv$') # get rid of processed files only
  sapply(f, FUN=function(x) { 
    if(debug) if(debug) message(paste('Deleting', x))
    unlink(x) 
  })
  # now download the huge daily dump file
  dump.file <- ''
  dump.file <- paste(data.directory, '/', symbol, '-dump.csv', sep='')
  if(download.daily.dump) {
    if(debug) message(paste0('download.data=TRUE... ', dump.file, ', downloading now...')) 
    dump.file <- download_daily_dump(symbol=symbol, data.directory=data.directory, overwrite=overwrite, debug=debug)  
  } else {
    if(!file.exists(dump.file)) {
      # file is missing download it!
      if(debug) message(paste0('Did not find dump file: ', dump.file, ', downloading now...')) 
      dump.file <- download_daily_dump(symbol=symbol, data.directory=data.directory, overwrite=overwrite, debug=debug)  
    }
  }
  # now let's process our dump file
  # split it into chunks of 20000 trades
  system(command=paste('split -l 20000 -a 5 -d ', dump.file, ' ', data.directory, '/', symbol, '-', sep=''), intern=TRUE)
  # rename all to .csv
  f <- list.files(path=data.directory, pattern=paste(symbol, '-[0-9]{5}$',sep=''), full.names=TRUE)
  sapply(f, FUN=function(x) { 
    last.date <- suppressWarnings(unlist(strsplit(system(command=paste('tail -n 1 ', x), intern=TRUE, ignore.stderr=TRUE),split=','))[1])
    first.date <- suppressWarnings(unlist(strsplit(system(command=paste('head -n 1 ', x), intern=TRUE, ignore.stderr=TRUE),split=','))[1])
    x.new <- gsub(pattern='[0-9]{5}', replacement=paste(first.date, last.date, sep='-'), x=x)
    x.new <- paste(x.new, 'csv', sep='.')
    file.rename(x, x.new) 
  })
  # now we will rebalance the ticks so we do not have the same timestamp spanning more than one file
  f <- list.files(path=data.directory, pattern='\\.csv$', full.names=TRUE)
  results<- sapply(f, FUN=function(x) { 
    # we put the try here because files in the original list may no longer exist after shuffling ticks and renaming
    try(silent=TRUE, expr={
      last.date <- suppressWarnings(unlist(strsplit(system(command=paste('tail -n 1 ', x), intern=TRUE, ignore.stderr=TRUE),split=','))[1])
      if(length(grep(last.date, x=f)) > 1) {
        # last.date of one file is the start.date of another, need to shuffle
        files <- sort(grep(last.date, x=f, value=TRUE))
        if(debug) message('=========================================================')
        if(debug) message(paste('Redistributing ticks in:', files, '\n'))
        # now grab the first couple of lines of the newer file and drop them in the older file
        data <- read.csv(files[2], header=FALSE)
        move <- data[ (data$V1 == last.date), ]
        if(debug) message('Moving chunk:')
        if(debug) message(paste0(move, ','))
        if(debug) message(paste('from', files[2], 'to', files[1]))
        write.table(x=move, file=files[1], append=TRUE, sep=',', row.names=FALSE, col.names=FALSE)
        # now fix the newer file
        if(debug) message(paste('Removing old file', files[2]))
        unlink(files[2], force=TRUE)
        move <- data[ (data$V1 > last.date), ]
        first.date <- move[1, 1]
        last.date <- move[nrow(move), 1]
        x.new <- paste(symbol, first.date, last.date, sep='-')
        x.new <- paste(x.new, '.csv', sep='')
        if(debug) message(paste('Creating new file', paste(data.directory, x.new, sep='/')))
        write.table(x=move, file=paste(data.directory, x.new, sep='/'), append=FALSE, sep=',', row.names=FALSE, col.names=FALSE)
        if(debug) message('=========================================================')
      }  
    })
  })
  dump.file
}

#' @title prepare_historical_data_fast
#' @description Download all historical trade data for a given symbol from bitcoincharts.com
#' @param symbol
#' @param data.directory
#' @param download.daily.dump
#' @param overwrite
#' @param debug
prepare_historical_data_fast <- function(symbol, 
                                    data.directory, 
                                    download.daily.dump, 
                                    overwrite,
                                    debug) 
{
  # now download the huge daily dump file
  dump.file <- ''
  dump.file <- paste(data.directory, '/', symbol, '-dump.csv', sep='')
  if(download.daily.dump) {
    if(debug) message(paste0('download.data=TRUE... ', dump.file, ', downloading now...')) 
    dump.file <- download_daily_dump(symbol=symbol, data.directory=data.directory, overwrite=overwrite, debug=debug)  
  } else {
    if(!file.exists(dump.file)) {
      # file is missing download it!
      if(debug) message(paste0('Did not find dump file: ', dump.file, ', downloading now...')) 
      dump.file <- download_daily_dump(symbol=symbol, data.directory=data.directory, overwrite=overwrite, debug=debug)  
    }
  }
  dump.file
}

#' @title get_trade_data
#' @description Download all historical trade data for a given symbol from bitcoincharts.com 
#' and split into 20000-line csv files. Then parse the files and return tick data for the specified
# time period
#' @param symbol
#' @param start.date
#' @param end.date
#' @param data.directory
get_trade_data <- function(symbol, 
                           start.date, 
                           end.date='', 
                           data.directory=paste(system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), symbol, sep='/')) 
{
  start.ts <- as.integer(as.POSIXct(as.Date(start.date), tz='UTC', origin='1970-01-01'))
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
    end.ts <- as.integer(as.POSIXct(as.Date(end.date), tz='UTC', origin='1970-01-01'))  
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
  #   if(debug) message("Will gather tick data from the following files:")
  f <- sort(list.files(path=data.directory, pattern='[0-9]{1,}\\.csv$', full.names=TRUE))
  #   if(debug) message(f[ start.idx:end.idx ])
  tickdata <- data.frame(timestamp=0,price=0,amount=0)
  for(fl in f[ start.idx:end.idx  ]) {
    #     if(debug) message(paste('Getting data from',fl))
    new.rows <- ''
    tryCatch( expr= { 
      new.rows <- read.csv(fl, header=FALSE, sep=',', colClasses=c('numeric','numeric','numeric'), col.names=c('timestamp','price','amount')) 
    }, warning=function(w) { 
      if(debug) message(paste('WARNING:', w$message, sep=' '))
      # this file needs newline char at end of file, add one and try to read it again
      if(grep(w$message,pattern='incomplete final line found')) {
        cat('\n', file=fl, sep='', append=TRUE)
        read.csv(fl, header=FALSE, sep=',', colClasses=c('numeric','numeric','numeric'), col.names=c('timestamp','price','amount')) 
      }
      if(debug) message('File fixed')
    }, error=function(e) {
      if(debug) message(paste0('ERROR: ',e$message)) 
    })
    tickdata <- rbind(tickdata,new.rows)
  }
  # skip the first row it was just dummy data
  tickdata <- tickdata[2:nrow(tickdata),]
  # now filter for date range from start
  tickdata <- tickdata[ tickdata$timestamp >= start.ts, ]
  # ok now if no end.date is specified we need to work our way backwards to the last available data
  if(end.date == '') {
    end.ts <- as.integer(as.POSIXct(Sys.time() + days(1), tz='UTC', origin='1970-01-01'))  
  } else {
    end.ts <- as.integer(as.POSIXct(as.Date(end.date) + days(1), tz='UTC', origin='1970-01-01'))  
#     if(as.integer(tickdata[ nrow(tickdata), 'timestamp']) > end.ts) {
#       # required data is already present, now filter out stuff that exceeds specified end.date
#       tickdata <- tickdata[ tickdata$timestamp <= end.ts, ]  
#     } else {
#       # we need to work backwards until we get the required data 
#       # earliest.timestamp <- end.ts - 1
#       # while(earliest.timestamp > end.ts) {
#       #    earliest.timestamp <- get_ticker(symbol=symbol, end=earliest.timestamp, data.directory=data.directory)   
#       # }
#     }
  }
  return(tickdata)
}

#' @title get_trade_data
#' @description Download all historical trade data for a given symbol from bitcoincharts.com. Then parse the files and return tick data for 
#' the specified time period
#' @param symbol
#' @param start.date
#' @param end.date
#' @param data.directory
get_trade_data_fast <- function(symbol, 
                           start.date, 
                           end.date='', 
                           data.directory=file.path(system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), symbol)) 
{
  start.ts <- as.integer(as.POSIXct(as.Date(start.date), tz='UTC', origin='1970-01-01'))
  end.ts <- as.integer(as.POSIXct(as.Date(end.date), tz='UTC', origin='1970-01-01'))  
  fl <- file.path(data.directory, paste0(symbol, '-dump.csv'))
  tickdata <- data.frame(fread(fl, header=FALSE, sep=',', colClasses=c('numeric','numeric','numeric')))
  tickdata <- setNames(tickdata, c('timestamp','price','amount'))
  tickdata <- tickdata[ (tickdata$timestamp >= start.ts) & (tickdata$timestamp <= end.ts), ]
  return(tickdata)
}

#' @title to.ohlc.xts
#' @description function which converts raw tick data to xts onject with OHLC
#' fill whether or nto to fill in missing values so you have a regularly spaced time series
#' @param ttime
#' @param tprice
#' @param tvolume
#' @param fmt character. Possible formats include: "%Y%m%d %H %M %S" (seconds), "%Y%m%d %H %M" (minutes), "%Y%m%d %H" (hours), "%Y%m%d" (daily)
#' @param align logical. Whether or not to align time series with align.time
#' @param fill logical. Whether or not to impute values for missing time periods.
to.ohlc.xts <- function(ttime, 
                        tprice, 
                        tvolume, 
                        fmt, 
                        align, 
                        fill) 
{
  if(fill) align <- TRUE
  ttime.int <- format(ttime,fmt)
  df <- data.frame(time = ttime[tapply(1:length(ttime),ttime.int,function(x) {head(x,1)})],
                   Open = tapply(tprice,ttime.int,function(x) {head(x,1)}), 
                   High = tapply(as.numeric(tprice),ttime.int,max),
                   Low = tapply(as.numeric(tprice),ttime.int,min),
                   Close = tapply(tprice,ttime.int,function(x) {tail(x,1)}),
                   Volume = tapply(as.numeric(tvolume),ttime.int,function(x) {sum(x)}),
                   Ticks = tapply(as.numeric(tvolume),ttime.int, length))
  # fill in any missing time slots and align along an appropriate boundary (minute, hour or day)
  if(align) {
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
  } else ohlc.xts <- as.xts(df[2:7], order.by=df$time)
  if(fill) {
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
  }
#   ohlc.xts[1:(nrow(ohlc.xts) - 1), ]
  ohlc.xts
}

# ----------------------------------------------------------------------------------------------
# EXPORTED (PUBLIC) FUNCTIONS
# ----------------------------------------------------------------------------------------------

#' @title Query bitcoincharts.com API
#' @description Query bitcoincharts.com API to obtain market data for bitcoin exchanges
#' @param symbol character. Supported exchanges can be obtained by calling the \code{'get_symbol_listing()'} method.
#' @param start.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format representing the start of the requested data series. Defaults to 30 days prior to current date.
#' @param end.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format representing the start of the requested data series. Defaults to current date + 1.
#' @param ohlc.frequency character. Supported values are \code{seconds}, \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years} 
#' @param align logical. Align time series index.
#' @param fill logical. Fill missing values.
#' @param data.directory character. Destination directory for downloaded data files. Defaults to package install extdata/marketdata directory.
#' @param download.data logical. Whether to download a fresh copy of the data file.
#' @param overwrite logical. Whether to overwrite the local copy of the data file.
#' @param auto.assign logical. Whether or not to auto-assign the variable to the environment specified in the \code{env} param
#' @param env character. Environment to auto.assign the return value to. Defaults to .GlobalEnv
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @export
#' @examples
#' \dontrun{
#' # Get one month of hourly market data for virtexCAD:
#' get_bitcoincharts_data('virtexCAD')
#' }
get_bitcoincharts_data <- function(symbol, 
                                   start.date=as.character(Sys.Date() - lubridate::days(30)), 
                                   end.date=as.character(Sys.Date() + lubridate::days(1)), 
                                   ohlc.frequency = 'hours', 
                                   align=TRUE,
                                   fill=FALSE,
                                   data.directory=paste(system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), symbol, sep='/'), 
                                   download.data=FALSE, 
                                   overwrite=FALSE, 
                                   auto.assign=FALSE, 
                                   env=.GlobalEnv,
                                   debug=FALSE) 
{
  if(system('which tail', ignore.stdout=TRUE, ignore.stderr=TRUE) != 0) stop('Unfortunately this package currently relies on the "tail" binary usually found on Linux/Unix systems...')
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
  prepare_historical_data(symbol=symbol, data.directory=data.directory, download.daily.dump=download.data, overwrite=overwrite, debug) 
  if(debug) message(paste('Getting tick data for: ', symbol, sep=''))
  tickdata <- get_trade_data(symbol=symbol, data.directory=data.directory, start.date=start.date, end.date=end.date)
  if(ohlc.frequency == 'seconds') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H %M %S", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - seconds(1)
  } else if(ohlc.frequency == 'minutes') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H %M", align, fill)
    index(ohlc.data.xts) <- index(ohlc.data.xts) - minutes(1)
  } else if(ohlc.frequency == 'hours') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - hours(1)
  } else if(ohlc.frequency == 'days') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - days(1)
  } else if(ohlc.frequency == 'months') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m", align, fill)  
  } else if(ohlc.frequency == 'years') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y", align, fill)  
  }
  ohlc.data.xts <- setNames(ohlc.data.xts, c('Open', 'High', 'Low', 'Close', 'Volume', 'Ticks'))
  # OK now we have data up to a certain point, if our end date is today, get every last bit of data
  if(as.Date(end.date) >= Sys.Date() & (ohlc.frequency %in% c('seconds', 'minutes', 'hours'))) {
    try(expr = { if(debug) message(paste('Getting most recent data for: ', symbol, sep=''))
      recent <- get_most_recent_ohlc(symbol=symbol, data.directory=data.directory, ohlc.frequency=ohlc.frequency, align=align, fill=fill, debug=debug)
      if(first(index(recent)) > last(index(ohlc.data.xts))) stop('There is a gap in the data, please rerun this function with download.data=TRUE and overwrite=TRUE')
      # now add the most recent on to what we have obtained from the dump
      ohlc.data.xts <- rbind(ohlc.data.xts, recent[ index(recent) > last(index(ohlc.data.xts)), ]) 
    })
  }
  if(!auto.assign) {
    # return to caller
    return(ohlc.data.xts)  
  } else {
    # assign it a-la quantmod
    assign(x=symbol, value=ohlc.data.xts, envir=env)
    return(NULL)
  }
}

#' @title Query bitcoincharts.com API using data.table methods instead of direct file manipulations
#' @description Query bitcoincharts.com API to obtain market data for bitcoin exchanges
#' @param symbol character. Supported exchanges can be obtained by calling the \code{'get_symbol_listing()'} method.
#' @param start.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format representing the start of the requested data series. Defaults to 30 days prior to current date.
#' @param end.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format representing the start of the requested data series. Defaults to current date + 1.
#' @param ohlc.frequency character. Supported values are \code{seconds}, \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years} 
#' @param align logical. Align time series index.
#' @param fill logical. Fill missing values.
#' @param data.directory character. Destination directory for downloaded data files. Defaults to package install extdata/marketdata directory.
#' @param download.data logical. Whether to download a fresh copy of the data file.
#' @param overwrite logical. Whether to overwrite the local copy of the data file.
#' @param auto.assign logical. Whether or not to auto-assign the variable to the environment specified in the \code{env} param
#' @param env character. Environment to auto.assign the return value to. Defaults to .GlobalEnv
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @export
#' @examples
#' \dontrun{
#' # Get one month of hourly market data for virtexCAD:
#' get_bitcoincharts_data('virtexCAD')
#' }
get_bitcoincharts_data_fast <- function(symbol, 
                                   start.date=as.character(Sys.Date() - lubridate::days(30)), 
                                   end.date=as.character(Sys.Date() + lubridate::days(1)), 
                                   ohlc.frequency = 'hours', 
                                   align=TRUE,
                                   fill=FALSE,
                                   data.directory=paste(system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), symbol, sep='/'), 
                                   download.data=FALSE, 
                                   overwrite=FALSE, 
                                   auto.assign=FALSE, 
                                   env=.GlobalEnv,
                                   debug=FALSE) 
{
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
  prepare_historical_data_fast(symbol=symbol, data.directory=data.directory, download.daily.dump=download.data, overwrite=overwrite, debug) 
  if(debug) message(paste('Getting tick data for: ', symbol, sep=''))
  tickdata <- get_trade_data_fast(symbol=symbol, data.directory=data.directory, start.date=start.date, end.date=end.date)
  if(ohlc.frequency == 'seconds') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H %M %S", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - seconds(1)
  } else if(ohlc.frequency == 'minutes') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H %M", align, fill)
    index(ohlc.data.xts) <- index(ohlc.data.xts) - minutes(1)
  } else if(ohlc.frequency == 'hours') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - hours(1)
  } else if(ohlc.frequency == 'days') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - days(1)
  } else if(ohlc.frequency == 'months') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m", align, fill)  
  } else if(ohlc.frequency == 'years') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y", align, fill)  
  }
  ohlc.data.xts <- setNames(ohlc.data.xts, c('Open', 'High', 'Low', 'Close', 'Volume', 'Ticks'))
  # OK now we have data up to a certain point, if our end date is today, get every last bit of data
  if(as.Date(end.date) >= Sys.Date() & (ohlc.frequency %in% c('seconds', 'minutes', 'hours'))) {
    try(expr = { if(debug) message(paste('Getting most recent data for: ', symbol, sep=''))
      recent <- get_most_recent_ohlc_fast(symbol=symbol, data.directory=data.directory, ohlc.frequency=ohlc.frequency, align=align, fill=fill, debug=debug)
      if(first(index(recent)) > last(index(ohlc.data.xts))) stop('There is a gap in the data, please rerun this function with download.data=TRUE and overwrite=TRUE')
      # now add the most recent on to what we have obtained from the dump
      ohlc.data.xts <- rbind(ohlc.data.xts, recent[ index(recent) > last(index(ohlc.data.xts)), ]) 
    })
  }
  if(!auto.assign) {
    # return to caller
    return(ohlc.data.xts)  
  } else {
    # assign it a-la quantmod
    assign(x=symbol, value=ohlc.data.xts, envir=env)
    return(NULL)
  }
}

#' @title load data for all known symbols into the global environment
#' @description load data for all known symbols into the global environment
#' @param start.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format representing the start of the requested data series. Defaults to 30 days prior to current date.
#' @param end.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format representing the start of the requested data series. Defaults to current date + 1.
#' @param ohlc.frequency character. Supported values are \code{seconds}, \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years} 
#' @param align logical. Align time series index.
#' @param fill logical. Fill missing values.
#' @param data.directory character. Destination directory for downloaded data files. Defaults to package install extdata/marketdata directory.
#' @param download.data logical. Whether to download a fresh copy of the data file.
#' @param overwrite logical. Whether to overwrite the local copy of the data file.
#' @param env character. Environment to auto.assign the return value to. Defaults to .GlobalEnv
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @export
#' @examples
#' \dontrun{
#' Load all data available up to the current minuute on bitcoincharts.com into the global environment
#' load_all_data()
#' }
load_all_data <- function(start.date=as.character(Sys.Date() - days(30)), 
                          end.date=as.character(Sys.Date() + days(1)), 
                          ohlc.frequency='days', 
                          align=TRUE,
                          fill=FALSE,
                          data.base.dir=system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), 
                          download.data=FALSE,
                          overwrite=FALSE,
                          env=.GlobalEnv,
                          debug=FALSE) 
{
  if(system('which tail', ignore.stdout=TRUE, ignore.stderr=TRUE) != 0) stop('Unfortunately this package currently relies on the "tail" binary usually found on Linux/Unix systems...')
  all.symbols <- get_symbol_listing()
  dev.null <- sapply(all.symbols, function(x) {
    if(debug) message(paste('Loading', x, '...'))
    tryCatch(expr={
      get_bitcoincharts_data(symbol=x, data.directory=paste(data.base.dir, x, sep='/'), start.date=start.date, end.date=end.date, ohlc.frequency=ohlc.frequency, align=align, fill=fill, download.data=download.data, overwrite=overwrite, auto.assign=TRUE, env=env)
    }, error=function(e) {
      if(debug) message(paste0('Failed to load data for ', x, ', error = ', e))
    })
  })
}

#' @title Download single exchange data file from bitcoincharts.com API
#' @description Download single exchange data file from bitcoincharts.com API
#' @param symbol character. Supported exchanges can be obtained by calling the \code{'get_symbol_listing()'} method.
#' @param data.directory character. Destination directory for downloaded data files. Defaults to package install extdata/marketdata for the symbol specified directory.
#' @param overwrite logical. Whether to overwrite the local copy of the data file.
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://api.bitcoincharts.com/v1/csv/}}
#' @export
#' @examples
#' \dontrun{
#' # Get the full dump of all MTGOX USD transactions
#' download_daily_dump(symbol='mtgoxUSD', data.directory='/home/user/Desktop')
#' }
download_daily_dump <- function(symbol, 
                                data.directory=paste(system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), symbol, sep='/'), 
                                overwrite=FALSE, 
                                debug=FALSE) 
{
  full.path <- paste(data.directory, '/', symbol, '-dump.csv.gz', sep='')
  # make sure the directory exists
  if(!file.exists(data.directory)) {
    if(debug) message(paste0('Creating missing data directory ', data.directory))
    dir.create(data.directory)
  } 
  if(file.exists(full.path)) {
    if(!overwrite) {
      if(debug) message(paste0(full.path, ' already exists, overwrite=FALSE, skipping download and returning file path to caller.'))
      return(full.path)
    } else {
      if(debug) message(paste0(full.path, ' already exists, overwrite=TRUE, deleting current data file and downloading.'))
      unlink(full.path)
    }
  }
  url <- paste('http://api.bitcoincharts.com/v1/csv/', symbol, '.csv.gz', sep='')
  download.file(url, destfile=full.path, method='auto', quiet=!debug, mode="w", cacheOK=TRUE, extra=getOption("download.file.extra"))
  if(debug) message(paste0('Data file for', symbol, ' downloaded to ', full.path))
  # now un gzip the file
  new.path <- str_replace(full.path, '.gz', '')
  gunzip(filename=full.path, destname=new.path, overwrite=TRUE)
  return(new.path)
}

#' @title Get total number of trades ever executed on a given exchange
#' @description Get total number of trades ever executed on a given exchange by counting lines in dump file
#' @param symbol character. Symbol to get total trade count for
#' @param base.data.directory character. Destination directory for downloaded data files. Defaults to package install extdata/marketdata directory.
#' @export
#' @examples
#' \dontrun{
#' # Download all available market data in one fell swoop:
#' download_all_daily_dumps()
#' }
get_all_time_trade_count <- function(symbol, 
                                     base.data.directory=system.file('extdata', 
                                                                     'market-data', 
                                                                     mustWork=TRUE, 
                                                                     package='bitcoinchartsr'),
                                     debug = FALSE) {
  dir.name <- file.path(base.data.directory, symbol)
  file.name <- file.path(dir.name, paste0(symbol, '-dump.csv'))
  if(!file.exists(file.name)) { download_daily_dump(symbol, overwrite = TRUE); if(debug) message(paste('Downloading data for', symbol)); }
  return(nrow(fread(file.name)))
}

#' @title Download all available data
#' @description Download all available data for all markets in one fell swoop. Caution! Some exchange data files are > 500 MB!!!
#' @param base.data.directory character. Destination directory for downloaded data files. Defaults to package install extdata/marketdata directory.
#' @param overwrite logical. Whether to overwrite the local copy of the data file.
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://api.bitcoincharts.com/v1/csv/}}
#' @export
#' @examples
#' \dontrun{
#' # Download all available market data in one fell swoop:
#' download_all_daily_dumps()
#' }
download_all_daily_dumps <- function(base.data.directory=system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), 
                                     overwrite=FALSE, 
                                     debug=FALSE) 
{
  tryCatch(expr={
    all.symbols <- get_symbol_listing(debug)
    sapply(all.symbols, function(x) {
      # if data dir doesn't exist create it
      dir.name <- paste(base.data.directory, x, sep='/')
      if(!file.exists(dir.name)) {
        if(debug) message(paste0('Creating base data directory ', dir.name))
        res <- try(dir.create(dir.name, showWarnings=TRUE))
        if(class(res) == 'try-error') stop(paste0('Creation of base data directory ', dir.name, ' failed. Can not proceed!'))
      }
      tryCatch(expr={
        file.name <- download_daily_dump(symbol=x, data.directory=dir.name, overwrite=overwrite, debug)
      }, error=function(e) {
        if(debug) message(paste0('Failed to download data for symbol ', x, ', error = ', e))
      })
    })
  }, error=function(e) {
    if(debug) message(paste0('ABORTING! Failed to get symbol listing, error = ', e))
  })
}

#' @title Get list of all currently available market symbols
#' @description Get list of all currently available market symbols from http://bitcoincharts.com/markets
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://bitcoincharts.com/markets}}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_symbol_listing()
#' }
get_symbol_listing <- function(debug=FALSE) 
{
  markets.url='http://bitcoincharts.com/markets/'
  if(debug) message(paste0('Getting updated symbol list from bitcoincharts.com.'))
  txt <- NA
  tryCatch(expr={
    txt <- getURL(markets.url)
  }, error=function(e) {
    stop(paste0('Failed to consult online listing of market symbols, error = ', e))
  })
  xmltext <- htmlParse(txt, asText=TRUE)
  xmltable <- xpathApply(xmltext, "//table//tr//td//nobr//a") # use relenium to get exact xpath
  all.symbols <- sort(unname(unlist(lapply(xmltable, FUN=function(x) { str_replace(str_replace(xmlAttrs(x)['href'], pattern='/markets/', replacement=''), pattern='\\.html', replacement='') }))))
  return(all.symbols)
}

#' @title Get detailed information for a specified exchange
#' @description Get detailed listing of all currently available exchanges from http://bitcoincharts.com/markets
#' @param symbol character. Exchange you wish to obtain info for
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://bitcoincharts.com/markets}}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_exchange_info()
#' }
get_exchange_info <- function(symbol, debug=FALSE) 
{
  markets.url <- 'http://bitcoincharts.com/markets/'
  markets.url <- paste0(markets.url, '/', symbol, '.html')
  txt <- getURL(markets.url)
  if(txt == '') return(NA)
  xmltext <- htmlParse(txt, asText=TRUE)
  labels <- xmlApply(xpathApply(xmltext, "//div//div//p//label"), xmlValue)
  vals <- xmlApply(xpathApply(xmltext, "//div//div//p//span"), xmlValue)
  return(cbind(labels, vals))
}

#' @title Get total mined coins from site header table
#' @description 
#' @references \url{http://bitcoincharts.com}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_total_mined_coins()
#' }
get_total_mined_coins <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  txt <- getURL(markets.url)
  xmltext <- htmlParse(txt, asText=TRUE)
  tbls <- readHTMLTable(xmltext)
  return(tbls[[1]])
}

#' @title Get current network difficulty from site header
#' @description 
#' @references \url{http://bitcoincharts.com}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_current_difficulty()
#' }
get_current_difficulty <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  txt <- getURL(markets.url)
  xmltext <- htmlParse(txt, asText=TRUE)
  tbls <- readHTMLTable(xmltext)
  return(tbls[[2]])
}

#' @title Get total network hashing power from site header
#' @description 
#' @references \url{http://bitcoincharts.com}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_total_network_hashing_power()
#' }
get_total_network_hashing_power <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  txt <- getURL(markets.url)
  xmltext <- htmlParse(txt, asText=TRUE)
  tbls <- readHTMLTable(xmltext)
  return(tbls[[3]])
}

#' @title Get snapshots of all markets listed on the site
#' @description 
#' @references \url{http://bitcoincharts.com}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_markets_snapshot()
#' }
get_markets_snapshot <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  txt <- getURL(markets.url)
  xmltext <- htmlParse(txt, asText=TRUE)
  tbls <- readHTMLTable(xmltext)
  tbl <- tbls[[4]]
  tbl <- apply(tbl, 2, str_trim)
  tbl <- apply(tbl, 2, str_replace_all, '\n|\t', ' ')
  tbl <- apply(tbl, 2, str_replace_all, ' {2,}', ' ')  
  tbl <- data.frame(tbl, stringsAsFactors = FALSE)
  symbol <- sapply(str_split(tbl$Symbol, ' '), last)
  name <- sapply(sapply(str_split(tbl$Symbol, ' '), 
                        function(x) x[ 1:(length(x) - 1) ]), paste0, collapse = ' ')
  last.price <- sapply(str_split(tbl$Latest.Price, ' '), '[', 1)
  last.price.time <- sapply(sapply(str_split(tbl$Latest.Price, ' '), 
                                   function(x) x[ 2:length(x) ]), paste0, collapse = ' ')
  avgs.30.day <- data.frame(matrix(unlist(str_split(tbl$Average, ' ')), 
                                   ncol = 3, byrow = TRUE), stringsAsFactors = FALSE)
  vols.30.day <- data.frame(matrix(unlist(str_split(tbl$Volume, ' ')), 
                                   ncol = 3, byrow = TRUE), stringsAsFactors = FALSE)
  low.high.30.day <- data.frame(matrix(unlist(str_split(tbl$Low.High, ' ')), 
                                       ncol = 2, byrow = TRUE), stringsAsFactors = FALSE)
  avgs.24.hr <- data.frame(matrix(unlist(str_split(str_replace(tbl$X24h.Avg., '^—$', 'NA NA NA'), ' ')), 
                                  ncol = 3, byrow = TRUE), stringsAsFactors = FALSE)
  vols.24.hr <- data.frame(matrix(unlist(str_split(tbl$Volume.1, ' ')), 
                                  ncol = 3, byrow = TRUE), stringsAsFactors = FALSE)
  low.high.24.hr <- data.frame(matrix(unlist(str_split(tbl$Low.High.1, ' ')), 
                                      ncol = 2, byrow = TRUE), stringsAsFactors = FALSE)  
  tbl <- cbind(change = tbl$V1, symbol, name, last.price, last.price.time, avgs.30.day,
               tbl$Bid, tbl$Ask, vols.30.day, low.high.30.day, avgs.24.hr, vols.24.hr, 
               low.high.24.hr)
  tbl <- apply(tbl, 2, str_replace_all, ',|%', '')
  tbl <- data.frame(tbl, stringsAsFactors = FALSE)
  tbl <- setNames(tbl, c('change', 'symbol', 'name', 'last.price', 'last.price.when', 
                         'price.30.day', 'price.30.day.change', 'price.30.day.change.percent',
                         'bid', 'ask', 
                         'vol.30.day', 'vol.30.day.change', 'vol.30.day.currency',
                         'low.30.day', 'high.30.day', 
                         'price.24.hr', 'price.24.hr.change', 'price.24.hr.change.percent',
                         'vol.24.hr', 'vol.24.hr.change', 'vol.24.hr.currency',
                         'low.24.hr', 'high.24.hr'))
  return(tbl)
}

#' @title Utility function which returns the most recent OHLC candle
#' @description Utility function which returns the most recent OHLC candle from the bitcoincharts.com API
#' @param symbol character. Supported exchanges can be obtained by calling the \code{'get_symbol_listing()'} method.
#' @param ohlc.frequency character. Supported values are \code{seconds}, \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years} 
#' @param data.directory character. Destination directory for downloaded data files. Defaults to package install extdata/marketdata directory.
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://bitcoincharts.com/markets}}
#' @export
#' @examples
#' \dontrun{
#' # Get most recent 1-day candle from cavirtex.com:
#' get_most_recent_trade('virtexCAD', ohlc.frequency='days')
#' }
get_most_recent_trade <- function(symbol, 
                                  ohlc.frequency='hours',
                                  align=TRUE,
                                  fill=FALSE,
                                  data.directory=paste(system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), symbol, sep='/'),
                                  debug=FALSE) 
{
  if(system('which tail', ignore.stdout=TRUE, ignore.stderr=TRUE) != 0) stop('Unfortunately this function currently relies on the "tail" binary usually found on Linux/Unix systems. "tail" was not found on your system path!')
  if(system('which head', ignore.stdout=TRUE, ignore.stderr=TRUE) != 0) stop('Unfortunately this function currently relies on the "head" binary usually found on Linux/Unix systems. "head" was not found on your system path!')
  call <- match.call()
  if(!(ohlc.frequency %in% c('seconds', 'minutes', 'hours', 'days', 'months', 'years'))) {
    stop("OHLC frequency must be one of following: seconds, minutes, hours, days, months, years")
  }
  last(get_most_recent_ohlc(symbol=symbol, data.directory=data.directory, ohlc.frequency=ohlc.frequency, align=align, fill=fill, debug=debug))
}

#' @title Get OHLC for last 2000 trades 
#' @description Utility function which returns the most recent OHLC candle from the bitcoincharts.com API
#' @param symbol character. Supported exchanges can be obtained by calling the \code{'get_symbol_listing()'} method.
#' @param data.directory character. Destination directory for downloaded data files. Defaults to package install extdata/marketdata directory.
#' @param ohlc.frequency character. Supported values are \code{seconds}, \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years}
#' @param align logical. Align time series index.
#' @param fill logical. Fill missing values. 
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://api.bitcoincharts.com/v1/trades.csv?symbol=SYMBOL[&end=UNIXTIME]}}
#' @export
#' @examples
#' \dontrun{
#' # Get most recent 1-day candle from cavirtex.com:
#' get_most_recent_trade('virtexCAD', ohlc.frequency='days')
#' }
get_most_recent_ohlc <- function(symbol, 
                                 data.directory=system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), 
                                 ohlc.frequency='hours', 
                                 align=TRUE,
                                 fill=FALSE,
                                 debug=FALSE) 
{
  if(system('which tail', ignore.stdout=TRUE, ignore.stderr=TRUE) != 0) stop('Unfortunately this function currently relies on the "tail" binary usually found on Linux/Unix systems. "tail" was not found on your system path!')
  if(system('which head', ignore.stdout=TRUE, ignore.stderr=TRUE) != 0) stop('Unfortunately this function currently relies on the "head" binary usually found on Linux/Unix systems. "head" was not found on your system path!')
  call <- match.call()
  if(!(ohlc.frequency %in% c('seconds', 'minutes', 'hours', 'days', 'months', 'years'))) {
    stop("OHLC frequency must be one of following: seconds, minutes, hours, days, months, years")
  }
  # make sure the directory exists
  if(!file.exists(data.directory)) {
    if(debug) message(paste0('Creating missing data directory ', data.directory))
    dir.create(data.directory)
  } 
  file.name <- paste(symbol, 'XXXXXXXXXX', sep='-')
  file.name <- paste(file.name, 'csv', sep='.')
  ColClasses = c('numeric','numeric','numeric')
  url <- paste('http://api.bitcoincharts.com/v1/trades.csv?symbol=', symbol, sep='')
  full.path <- paste(data.directory,file.name,sep='/')
  download.file(url, destfile=full.path, method='auto', quiet = !debug, mode = "w", cacheOK = TRUE, extra = getOption("download.file.extra"))
  # now we need to find the last timestamp downloaded to complete the file name
  last.line <- suppressWarnings(system(command=paste('tail -n 1 ', full.path), intern=TRUE, ignore.stderr=TRUE))
  first.line <- suppressWarnings(system(command=paste('head -n 1 ', full.path), intern=TRUE, ignore.stderr=TRUE))
  earliest.timestamp <- unlist(str_split(last.line,pattern=','))[1]
  latest.timestamp <- unlist(str_split(first.line,pattern=','))[1]
  file.name <- gsub(file.name,pattern='XXXXXXXXXX',replacement=earliest.timestamp)
  file.name <- gsub(file.name,pattern='YYYYYYYYYY',replacement=latest.timestamp)
  new.full.path <- paste(data.directory,file.name,sep='/')
  file.rename(full.path, new.full.path)
  tickdata <- read.csv(new.full.path, header=FALSE, sep=',', colClasses=c('numeric','numeric','numeric'), col.names=c('timestamp','price','amount'))
  if(ohlc.frequency == 'seconds') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H %M %S", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - seconds(1)
  } else if(ohlc.frequency == 'minutes') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H %M", align, fill)
    index(ohlc.data.xts) <- index(ohlc.data.xts) - minutes(1)
  } else if(ohlc.frequency == 'hours') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - hours(1)
  } else if(ohlc.frequency == 'days') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - days(1)
  } else if(ohlc.frequency == 'months') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m", align, fill)  
  } else if(ohlc.frequency == 'years') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y", align, fill)  
  }
  ohlc.data.xts <- setNames(ohlc.data.xts, c('Open', 'High', 'Low', 'Close', 'Volume', 'Ticks'))
  unlink(new.full.path, force=TRUE)
  return(ohlc.data.xts)
}

#' @title Get OHLC for last 2000 trades using data.table functions instead of direct file manipulation
#' @description Utility function which returns the most recent OHLC candle from the bitcoincharts.com API
#' @param symbol character. Supported exchanges can be obtained by calling the \code{'get_symbol_listing()'} method.
#' @param data.directory character. Destination directory for downloaded data files. Defaults to package install extdata/marketdata directory.
#' @param ohlc.frequency character. Supported values are \code{seconds}, \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years}
#' @param align logical. Align time series index.
#' @param fill logical. Fill missing values. 
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://api.bitcoincharts.com/v1/trades.csv?symbol=SYMBOL[&end=UNIXTIME]}}
#' @export
#' @examples
#' \dontrun{
#' # Get most recent 1-day candle from cavirtex.com:
#' get_most_recent_trade('virtexCAD', ohlc.frequency='days')
#' }
get_most_recent_ohlc_fast <- function(symbol, 
                                 data.directory=system.file('extdata', 'market-data', mustWork=TRUE, package='bitcoinchartsr'), 
                                 ohlc.frequency='hours', 
                                 align=TRUE,
                                 fill=FALSE,
                                 debug=FALSE) 
{
  call <- match.call()
  if(!(ohlc.frequency %in% c('seconds', 'minutes', 'hours', 'days', 'months', 'years'))) {
    stop("OHLC frequency must be one of following: seconds, minutes, hours, days, months, years")
  }
  # make sure the directory exists
  if(!file.exists(data.directory)) {
    if(debug) message(paste0('Creating missing data directory ', data.directory))
    dir.create(data.directory)
  } 
  url <- paste('http://api.bitcoincharts.com/v1/trades.csv?symbol=', symbol, sep='')
  file.name <- paste0(symbol, '-recent.csv')
  full.path <- file.path(data.directory, file.name)
  download.file(url, destfile=full.path, method='auto', quiet = !debug, mode = "w", cacheOK = TRUE, extra = getOption("download.file.extra"))
  tickdata <- fread(full.path, header=FALSE, sep=',', colClasses=c('numeric','numeric','numeric'))
  tickdata <- setNames(tickdata, c('timestamp','price','amount'))
  if(ohlc.frequency == 'seconds') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H %M %S", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - seconds(1)
  } else if(ohlc.frequency == 'minutes') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H %M", align, fill)
    index(ohlc.data.xts) <- index(ohlc.data.xts) - minutes(1)
  } else if(ohlc.frequency == 'hours') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d %H", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - hours(1)
  } else if(ohlc.frequency == 'days') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m%d", align, fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - days(1)
  } else if(ohlc.frequency == 'months') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y%m", align, fill)  
  } else if(ohlc.frequency == 'years') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),tz='UTC',origin='1970-01-01'),as.numeric(tickdata$price),as.numeric(tickdata$amount),"%Y", align, fill)  
  }
  ohlc.data.xts <- setNames(ohlc.data.xts, c('Open', 'High', 'Low', 'Close', 'Volume', 'Ticks'))
  unlink(full.path, force=TRUE)
  return(ohlc.data.xts)
}