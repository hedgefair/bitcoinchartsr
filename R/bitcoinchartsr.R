# library(quantmod)
# library(XML)
# library(RCurl)
# library(stringr)
# library(lubridate)
# library(R.utils)
# library(data.table)
# library(xts)
# options(scipen = 500)

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
#' @import stringr
download_daily_dump <- function(symbol, 
                                data.directory = file.path(system.file('extdata', 
                                                                       'market-data', 
                                                                       mustWork = TRUE, 
                                                                       package = 'bitcoinchartsr'), 
                                                           symbol), 
                                overwrite = TRUE, 
                                debug = FALSE) {
  full.path <- file.path(data.directory, paste0(symbol, '-dump.csv.gz'))
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
  url <- paste0('http://api.bitcoincharts.com/v1/csv/', symbol, '.csv.gz')
  download.file(url, destfile = full.path, method = 'auto', quiet = !debug, 
                mode = "w", cacheOK = TRUE, extra = getOption("download.file.extra"))
  if(debug) message(paste0('Data file for', symbol, ' downloaded to ', full.path))
  # now un gzip the file
  new.path <- str_replace(full.path, '.gz', '')
  gunzip(filename = full.path, destname = new.path, overwrite=TRUE)
  return(new.path)
}

#' @title Download all available data
#' @description Download all available data for all markets in one fell swoop. 
#' Caution! Some exchange data files are > 500 MB!!!
#' @param base.data.directory character. Destination directory for downloaded 
#' data files. Defaults to package install extdata/marketdata directory.
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
download_all_daily_dumps <- function(base.data.directory = system.file('extdata', 
                                                                       'market-data', 
                                                                       mustWork = TRUE, 
                                                                       package = 'bitcoinchartsr'), 
                                     overwrite = TRUE, 
                                     debug = TRUE) {
  tryCatch(expr={
    all.symbols <- get_symbol_listing(debug)
    sapply(all.symbols, function(x) {
      # if data dir doesn't exist create it
      dir.name <- file.path(base.data.directory, x)
      if(!file.exists(dir.name)) {
        if(debug) message(paste0('Creating base data directory ', dir.name))
        res <- try(dir.create(dir.name, showWarnings = TRUE))
        if(class(res) == 'try-error') stop(paste0('Creation of base data directory ', 
                                                  dir.name, 
                                                  ' failed. Can not proceed!'))
      }
      tryCatch(expr = {
        file.name <- download_daily_dump(symbol = x, data.directory = dir.name, 
                                         overwrite = overwrite, debug)
      }, error=function(e) {
        if(debug) message(paste0('Failed to download data for symbol ', 
                                 x, ', error = ', e))
      })
    })
  }, error=function(e) {
    if(debug) message(paste0('ABORTING! Failed to get symbol listing, error = ', e))
  })
}

#' @title get_trade_data
#' @description Download all historical trade data for a given symbol from 
#' bitcoincharts.com. Then parse the files and return tick data for the specified 
#' time period
#' @param symbol
#' @param start.date
#' @param end.date
#' @param data.directory
#' @import lubridate 
#' @importFrom data.table fread
get_trade_data <- function(symbol, 
                           start.date = as.character(as.Date(now() - days(30))), 
                           end.date = as.character(as.Date(now())), 
                           data.directory = file.path(system.file('extdata', 
                                                                  'market-data', 
                                                                  mustWork = TRUE, 
                                                                  package = 'bitcoinchartsr'), 
                                                      symbol)) {
  start.ts <- as.integer(as.POSIXct(as.Date(start.date), 
                                    tz = 'UTC', 
                                    origin = '1970-01-01'))
  end.ts <- as.integer(as.POSIXct(as.Date(end.date), 
                                  tz = 'UTC', 
                                  origin = '1970-01-01'))  
  fl <- file.path(data.directory, 
                  paste0(symbol, '-dump.csv'))
  if(!exists(fl)) download_daily_dump(symbol)
  tickdata <- data.frame(fread(fl, 
                               header = FALSE, 
                               sep = ',', 
                               colClasses = c('numeric', 'numeric', 'numeric')), 
                         stringsAsFactors = FALSE)
  tickdata <- setNames(tickdata, c('timestamp', 'price', 'amount'))
  tickdata <- tickdata[ which((tickdata$timestamp >= start.ts) & 
                                (tickdata$timestamp <= end.ts)), ]
  return(tickdata)
}

#' @title to.ohlc.xts
#' @description function which converts raw tick data to xts onject with OHLC
#' fill whether or nto to fill in missing values so you have a regularly spaced 
#' time series
#' @param ttime
#' @param tprice
#' @param tvolume
#' @param fmt character. Possible formats include: "%Y%m%d %H %M %S" (seconds), 
#' "%Y%m%d %H %M" (minutes), "%Y%m%d %H" (hours), "%Y%m%d" (daily)
#' @param align logical. Whether or not to align time series with align.time
#' @param fill logical. Whether or not to impute values for missing time periods.
#' @import xts zoo
to.ohlc.xts <- function(ttime, 
                        tprice, 
                        tvolume, 
                        fmt, 
                        align = FALSE,  
                        fill = FALSE) {
  if(fill) align <- TRUE
  ttime.int <- format(ttime,fmt)
  df <- data.frame(time = ttime[ tapply(1:length(ttime), ttime.int, function(x) { head(x, 1) }) ],
                   Open = tapply(tprice, ttime.int, function(x) { head(x, 1) }), 
                   High = tapply(as.numeric(tprice), ttime.int, max),
                   Low = tapply(as.numeric(tprice), ttime.int, min),
                   Close = tapply(tprice, ttime.int, function(x) { tail(x, 1) }),
                   Volume = tapply(as.numeric(tvolume), ttime.int, function(x) { sum(x) }),
                   Ticks = tapply(as.numeric(tvolume), ttime.int, length))
  # fill in any missing time slots and align along an appropriate boundary (minute, hour or day)
  if(align) {
    if(fmt == '%Y%m%d %H %M %S') {
      ohlc.xts <- align.time(as.xts(df[ 2:7 ], order.by = df$time), n = 1)  
      idx <- align.time(seq(start(ohlc.xts), end(ohlc.xts), by = 1), n = 1)
    } else if(fmt == '%Y%m%d %H %M') {
      ohlc.xts <- align.time(as.xts(df[ 2:7 ], order.by = df$time), n = 60)  
      idx <- align.time(seq(start(ohlc.xts), end(ohlc.xts), by = 60), n = 60)
    } else if(fmt == '%Y%m%d %H') {
      ohlc.xts <- align.time(as.xts(df[ 2:7 ], order.by = df$time), n = 60*60) 
      idx <- align.time(seq(start(ohlc.xts), end(ohlc.xts), by = 60*60), n = 60*60)
    } else if(fmt == '%Y%m%d') {
      ohlc.xts <- align.time(as.xts(df[ 2:7 ], order.by = df$time), n = 60*60*24) 
      idx <- align.time(seq(start(ohlc.xts), end(ohlc.xts), by = 60*60*24), n = 60*60*24)
    } else if(fmt == '%Y%m') {
      ohlc.xts <- as.xts(df[ 2:7 ], order.by = df$time)
      return(ohlc.xts)
    } else if(fmt == '%Y') {
      ohlc.xts <- as.xts(df[ 2:7 ], order.by = df$time)
      return(ohlc.xts)
    }   
  } else ohlc.xts <- as.xts(df[ 2:7 ], order.by = df$time)
  if(fill) {
    empties <- data.frame(Open = rep(NA, times = length(idx)),
                          High = rep(NA, times = length(idx)),
                          Low = rep(NA, times = length(idx)),
                          Close = rep(NA, times = length(idx)),
                          Volume = rep(NA, times = length(idx)),
                          Ticks = rep(NA, times = length(idx)),
                          row.names = idx)
    empties <- as.xts(empties, order.by = idx)
    ohlc.xts <- merge(ohlc.xts, empties)[ , 1:6 ]
    # fill any NA values with previous available ticks close ( vol = 0, op, hi, lo all should equal previous close)
    missing.vals <- which(is.na(ohlc.xts$Open))
    ohlc.xts[ is.na(ohlc.xts$Volume), 'Volume' ] <- 0
    ohlc.xts[ is.na(ohlc.xts$Ticks), 'Ticks' ] <- 0
    ohlc.xts[ ,'Close' ] <- na.locf(ohlc.xts$Close)
    ohlc.xts[ missing.vals, c('Open', 'High', 'Low') ] <- ohlc.xts[ missing.vals, 'Close' ]  
  }
  ohlc.xts
}

#' @title Query bitcoincharts.com API
#' @description Query bitcoincharts.com API to obtain market data for bitcoin exchanges
#' @param symbol character. Supported exchanges can be obtained by calling the 
#' \code{'get_symbol_listing()'} method.
#' @param start.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format 
#' representing the start of the requested data series. Defaults to 30 days prior 
#' to current date.
#' @param end.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format 
#' representing the start of the requested data series. Defaults to current date + 1.
#' @param ohlc.frequency character. Supported values are \code{seconds}, 
#' \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years} 
#' @param align logical. Align time series index.
#' @param fill logical. Fill missing values.
#' @param data.directory character. Destination directory for downloaded data 
#' files. Defaults to package install extdata/marketdata directory.
#' @param download.data logical. Whether to download a fresh copy of the data file.
#' @param overwrite logical. Whether to overwrite the local copy of the data file.
#' @param auto.assign logical. Whether or not to auto-assign the variable to the 
#' environment specified in the \code{env} param
#' @param env character. Environment to auto.assign the return value to. 
#' Defaults to .GlobalEnv
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @export
#' @examples
#' \dontrun{
#' # Get one month of hourly market data for virtexCAD:
#' get_bitcoincharts_data('virtexCAD')
#' }
#' @import lubridate stringr xts zoo
get_bitcoincharts_data <- function(symbol, 
                                   start.date = as.character(as.Date(now() - days(30))), 
                                   end.date = as.character(as.Date(now() + days(1))), 
                                   ohlc.frequency = 'hours', 
                                   align = TRUE,
                                   fill = FALSE,
                                   data.directory = file.path(system.file('extdata', 
                                                                          'market-data', 
                                                                          mustWork = TRUE, 
                                                                          package = 'bitcoinchartsr'), 
                                                              symbol), 
                                   download.data = FALSE, 
                                   overwrite = FALSE, 
                                   auto.assign = FALSE, 
                                   env = .GlobalEnv,
                                   debug = FALSE) {
  # sanity checks
  if(!(ohlc.frequency %in% c('seconds', 'minutes', 'hours', 'days', 'months', 'years'))) {
    stop("OHLC frequency must be one of following: seconds, minutes, hours, days, months, years")
  }
  if(!(str_detect(start.date, pattern='[0-9]{4}-[0-9]{2}-[0-9]{2}'))) {
    stop("start.date must be in YYYY-MM-DD format")
  }
  if(!(str_detect(end.date, pattern='[0-9]{4}-[0-9]{2}-[0-9]{2}'))) {
    stop("end.date must be in YYYY-MM-DD format")
  }
  if(debug) message(paste('Getting tick data for: ', symbol, sep=''))
  tickdata <- get_trade_data(symbol = symbol, data.directory = data.directory, 
                             start.date = start.date, end.date = end.date)
  if(ohlc.frequency == 'seconds') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC', 
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y%m%d %H %M %S', 
                                 align, 
                                 fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - seconds(1)
  } else if(ohlc.frequency == 'minutes') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y%m%d %H %M', 
                                 align, 
                                 fill)
    index(ohlc.data.xts) <- index(ohlc.data.xts) - minutes(1)
  } else if(ohlc.frequency == 'hours') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y%m%d %H', 
                                 align, 
                                 fill) 
    index(ohlc.data.xts) <- index(ohlc.data.xts) - hours(1)
  } else if(ohlc.frequency == 'days') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y%m%d', 
                                 align, 
                                 fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - days(1)
  } else if(ohlc.frequency == 'months') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y%m', align, 
                                 fill)  
  } else if(ohlc.frequency == 'years') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y', 
                                 align, 
                                 fill)  
  }
  ohlc.data.xts <- setNames(ohlc.data.xts, 
                            c('Open', 'High', 'Low', 'Close', 'Volume', 'Ticks'))
  # OK now we have data up to a certain point, if our end date is today, get every last bit of data
  if(as.Date(end.date) >= Sys.Date() & (ohlc.frequency %in% c('seconds', 'minutes', 'hours'))) {
    try(expr = { 
      if(debug) message(paste0('Getting most recent data for: ', symbol))
      recent <- get_most_recent_ohlc(symbol = symbol, 
                                     start.date = as.character(as.Date(last(index(ohlc.data.xts)))),
                                     data.directory = data.directory, 
                                     ohlc.frequency = ohlc.frequency, 
                                     align = align, 
                                     fill = fill, 
                                     debug = debug)
      if(first(index(recent)) > xts::last(index(ohlc.data.xts))) stop('There is a gap in the data, please rerun this function with download.data = TRUE and overwrite = TRUE')
      # now add the most recent on to what we have obtained from the dump
      ohlc.data.xts <- rbind(ohlc.data.xts, 
                             recent[ index(recent) > xts::last(index(ohlc.data.xts)), ]) 
    })
  } 
  #   if(last(index(ohlc.data.xts)) < as.POSIXct(end.date)) {
  #     browser()
  #     get_most_recent_ohlc(symbol = symbol, start.date = as.character(as.Date(last(index(ohlc.data.xts)))))
  #   }
  if(!auto.assign) {
    # return to caller
    return(ohlc.data.xts)  
  } else {
    # assign it a-la quantmod
    assign(x = symbol, value = ohlc.data.xts, envir = env)
    return(NULL)
  }
}

#' @title Utility function which returns the most recent OHLC candle
#' @description Utility function which returns the most recent OHLC candle from 
#' the bitcoincharts.com API
#' @param symbol character. Supported exchanges can be obtained by calling the 
#' \code{'get_symbol_listing()'} method.
#' @param ohlc.frequency character. Supported values are \code{seconds}, 
#' \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years} 
#' @param data.directory character. Destination directory for downloaded data 
#' files. Defaults to package install extdata/marketdata directory.
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://bitcoincharts.com/markets}}
#' @export
#' @examples
#' \dontrun{
#' # Get most recent 1-day candle from cavirtex.com:
#' get_most_recent_trade('virtexCAD', ohlc.frequency='days')
#' }
#' @import xts zoo 
get_most_recent_trade <- function(symbol, 
                                  ohlc.frequency = 'hours',
                                  align = TRUE,
                                  fill = FALSE,
                                  data.directory = file.path(system.file('extdata', 
                                                                         'market-data', 
                                                                         mustWork = TRUE, 
                                                                         package = 'bitcoinchartsr'), 
                                                             symbol),
                                  debug = FALSE) {
  if(!(ohlc.frequency %in% c('seconds', 'minutes', 'hours', 'days', 'months', 'years'))) {
    stop("OHLC frequency must be one of following: seconds, minutes, hours, days, months, years")
  }
  xts::last(get_most_recent_ohlc(symbol = symbol, data.directory = data.directory, 
                                 ohlc.frequency = ohlc.frequency, align = align, 
                                 fill = fill, debug = debug))
}

#' @title Get OHLC for last two days of trades via API
#' @description Utility function which returns the most recent OHLC candle from 
#' the bitcoincharts.com API
#' @param symbol character. Supported exchanges can be obtained by calling the 
#' \code{'get_symbol_listing()'} method.
#' @param data.directory character. Destination directory for downloaded data 
#' files. Defaults to package install extdata/marketdata directory.
#' @param ohlc.frequency character. Supported values are \code{seconds}, 
#' \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years}
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
#' @import lubridate zoo xts
get_most_recent_ohlc <- function(symbol, 
                                 start.date = as.character(as.Date(now() - days(2))),
                                 data.directory = system.file('extdata', 
                                                              'market-data', 
                                                              mustWork = TRUE, 
                                                              package = 'bitcoinchartsr'), 
                                 ohlc.frequency = 'hours', 
                                 align = TRUE,
                                 fill = FALSE,
                                 debug = FALSE) {
  if(!(ohlc.frequency %in% c('seconds', 'minutes', 'hours', 'days', 'months', 'years'))) {
    stop("OHLC frequency must be one of following: seconds, minutes, hours, days, months, years")
  }
  # make sure the directory exists
  if(!file.exists(data.directory)) {
    if(debug) message(paste0('Creating missing data directory ', data.directory))
    dir.create(data.directory)
  } 
  start.date <- as.integer(as.POSIXct(start.date))
  url <- paste0('http://api.bitcoincharts.com/v1/trades.csv?symbol=', symbol, '&start=', start.date)
  file.name <- paste0(symbol, '-recent.csv')
  full.path <- file.path(data.directory, file.name)
  download.file(url, destfile = full.path, method = 'auto', quiet = !debug, 
                mode = "w", cacheOK = TRUE, extra = getOption("download.file.extra"))
  tickdata <- data.frame(fread(full.path, 
                               header = FALSE, 
                               sep = ',', 
                               colClasses = c('numeric','numeric','numeric')),
                         stringsAsFactors = FALSE)
  tickdata <- setNames(tickdata, 
                       c('timestamp','price','amount'))
  if(ohlc.frequency == 'seconds') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y%m%d %H %M %S', 
                                 align, 
                                 fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - seconds(1)
  } else if(ohlc.frequency == 'minutes') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y%m%d %H %M', 
                                 align, 
                                 fill)
    index(ohlc.data.xts) <- index(ohlc.data.xts) - minutes(1)
  } else if(ohlc.frequency == 'hours') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y%m%d %H', 
                                 align, 
                                 fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - hours(1)
  } else if(ohlc.frequency == 'days') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y%m%d', 
                                 align, 
                                 fill)  
    index(ohlc.data.xts) <- index(ohlc.data.xts) - days(1)
  } else if(ohlc.frequency == 'months') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y%m', 
                                 align, 
                                 fill)  
  } else if(ohlc.frequency == 'years') {
    ohlc.data.xts <- to.ohlc.xts(as.POSIXct(as.numeric(tickdata$timestamp),
                                            tz = 'UTC',
                                            origin = '1970-01-01'),
                                 as.numeric(tickdata$price),
                                 as.numeric(tickdata$amount),
                                 '%Y', 
                                 align, 
                                 fill)  
  }
  ohlc.data.xts <- setNames(ohlc.data.xts, 
                            c('Open', 'High', 'Low', 'Close', 'Volume', 'Ticks'))
  unlink(full.path, force = TRUE)
  return(ohlc.data.xts)
}

#' @title load data for all known symbols into the global environment
#' @description load data for all known symbols into the global environment
#' @param start.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format 
#' representing the start of the requested data series. Defaults to 30 days prior 
#' to current date.
#' @param end.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format 
#' representing the start of the requested data series. Defaults to current date + 1.
#' @param ohlc.frequency character. Supported values are \code{seconds}, 
#' \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years} 
#' @param align logical. Align time series index.
#' @param fill logical. Fill missing values.
#' @param data.directory character. Destination directory for downloaded data 
#' files. Defaults to package install extdata/marketdata directory.
#' @param download.data logical. Whether to download a fresh copy of the data file.
#' @param overwrite logical. Whether to overwrite the local copy of the data file.
#' @param env character. Environment to auto.assign the return value to. 
#' Defaults to .GlobalEnv
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @export
#' @examples
#' \dontrun{
#' Load all data available up to the current minuute on bitcoincharts.com into 
#' the global environment
#' load_all_data()
#' }
#' @import lubridate xts zoo
load_all_data <- function(start.date = as.character(as.Date(now() - days(30))), 
                          end.date = as.character(as.Date(now() + days(1))), 
                          ohlc.frequency = 'days', 
                          align = TRUE,
                          fill = FALSE,
                          data.base.dir = system.file('extdata', 
                                                      'market-data', 
                                                      mustWork = TRUE, 
                                                      package = 'bitcoinchartsr'), 
                          download.data = FALSE,
                          overwrite = FALSE,
                          env = .GlobalEnv,
                          debug = FALSE) {
  all.symbols <- get_symbol_listing()
  dev.null <- sapply(all.symbols, function(x) {
    if(debug) message(paste('Loading', x, '...'))
    tryCatch(expr = {
      get_bitcoincharts_data(symbol = x, data.directory = file.path(data.base.dir, x), 
                             start.date = start.date, end.date = end.date, 
                             ohlc.frequency = ohlc.frequency, align = align, 
                             fill = fill, download.data = download.data, 
                             overwrite = overwrite, auto.assign = TRUE, env = env)
    }, error = function(e) {
      if(debug) message(paste0('Failed to load data for ', x, ', error = ', e))
    })
  })
}

#' @title Get total number of trades ever executed on a given exchange
#' @description Get total number of trades ever executed on a given exchange by 
#' counting lines in dump file
#' @param symbol character. Symbol to get total trade count for
#' @param base.data.directory character. Destination directory for downloaded 
#' data files. Defaults to package install extdata/marketdata directory.
#' @export
#' @examples
#' \dontrun{
#' # Download all available market data in one fell swoop:
#' download_all_daily_dumps()
#' }
#' @importFrom data.table fread
get_all_time_trade_count <- function(symbol, 
                                     base.data.directory = system.file('extdata', 
                                                                       'market-data', 
                                                                       mustWork = TRUE, 
                                                                       package = 'bitcoinchartsr'),
                                     debug = FALSE) {
  dir.name <- file.path(base.data.directory, symbol)
  file.name <- file.path(dir.name, paste0(symbol, '-dump.csv'))
  if(!file.exists(file.name)) { 
    download_daily_dump(symbol, overwrite = TRUE)
    if(debug) message(paste('Downloading data for', symbol)) 
  }
  return(nrow(data.frame(fread(file.name))))
}

#' @title Get list of all currently available market symbols
#' @description Get list of all currently available market symbols from 
#' http://bitcoincharts.com/markets
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://bitcoincharts.com/markets}}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_symbol_listing()
#' }
#' @import stringr XML
get_symbol_listing <- function(debug = FALSE) {
  markets.url = 'http://bitcoincharts.com/markets/'
  if(debug) message(paste0('Getting updated symbol list from bitcoincharts.com.'))
  txt <- NA
  tryCatch(expr = {
    txt <- getURL(markets.url)
  }, error = function(e) {
    stop(paste0('Failed to consult online listing of market symbols, error = ', e))
  })
  xmltext <- htmlParse(txt, asText = TRUE)
  xmltable <- xpathApply(xmltext, "//table//tr//td//nobr//a") # use relenium to get exact xpath
  all.symbols <- sort(unname(unlist(lapply(xmltable, FUN = function(x) { 
    str_replace(str_replace(xmlAttrs(x)[ 'href' ], 
                            pattern = '/markets/', 
                            replacement = ''), 
                pattern = '\\.html', 
                replacement = '') }))))
  return(all.symbols)
}

#' @title Get detailed information for a specified exchange
#' @description Get detailed listing of all currently available exchanges from 
#' http://bitcoincharts.com/markets
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
#' @import XML
get_exchange_info <- function(symbol, 
                              debug = FALSE) {
  markets.url <- 'http://bitcoincharts.com/markets/'
  markets.url <- paste0(markets.url, '/', symbol, '.html')
  txt <- getURL(markets.url)
  if(txt == '') return(NA)
  xmltext <- htmlParse(txt, asText = TRUE)
  labels <- xmlApply(xpathApply(xmltext, "//div//div//p//label"), xmlValue)
  vals <- xmlApply(xpathApply(xmltext, "//div//div//p//span"), xmlValue)
  return(cbind(labels, vals))
}

#' @title Get total mined coins from site header table
#' @description Get total mined coins from site header table
#' @references \url{http://bitcoincharts.com}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_total_mined_coins()
#' }
#' @import XML
get_total_mined_coins <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  txt <- getURL(markets.url)
  xmltext <- htmlParse(txt, asText = TRUE)
  tbls <- readHTMLTable(xmltext)
  return(tbls[[ 1 ]])
}

#' @title Get current network difficulty from site header
#' @description Get current network difficulty from site header
#' @references \url{http://bitcoincharts.com}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_current_difficulty()
#' }
#' @import XML
get_current_difficulty <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  txt <- getURL(markets.url)
  xmltext <- htmlParse(txt, asText = TRUE)
  tbls <- readHTMLTable(xmltext)
  return(tbls[[ 2 ]])
}

#' @title Get total network hashing power from site header
#' @description Get total network hashing power from site header
#' @references \url{http://bitcoincharts.com}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_total_network_hashing_power()
#' }
#' @import XML
get_total_network_hashing_power <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  txt <- getURL(markets.url)
  xmltext <- htmlParse(txt, asText = TRUE)
  tbls <- readHTMLTable(xmltext)
  return(tbls[[ 3 ]])
}

#' @title Get snapshots of all markets listed on the site
#' @description Get snapshots of all markets listed on the site
#' @references \url{http://bitcoincharts.com}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_markets_snapshot()
#' }
#' @import stringr XML
get_markets_snapshot <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  tbls <- readHTMLTable(markets.url)
  tbl <- tbls[[ 4 ]]
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
  tbl <- apply(tbl, 2, str_replace_all, '—', NA)
  tbl <- data.frame(tbl, stringsAsFactors = FALSE)
  tbl <- setNames(tbl, c('change', 'symbol', 'name', 'last.price', 'last.price.when', 
                         'price.30.day', 'price.30.day.change', 'price.30.day.change.percent',
                         'bid', 'ask', 
                         'vol.30.day.btc', 'vol.30.day.fiat', 'vol.30.day.currency',
                         'low.30.day', 'high.30.day', 
                         'price.24.hr', 'price.24.hr.change', 'price.24.hr.change.percent',
                         'vol.24.hr.btc', 'vol.24.hr.fiat', 'vol.24.hr.currency',
                         'low.24.hr', 'high.24.hr'))
  tbl <- transform(tbl, 
                   change = factor(str_trim(str_replace(change, '[A-Z]{3}', ''))),
                   name = factor(str_trim(name)),
                   last.price = as.numeric(last.price),
                   price.30.day = as.numeric(price.30.day),
                   price.30.day.change = as.numeric(price.30.day.change),
                   price.30.day.change.percent = as.numeric(price.30.day.change.percent),
                   bid = as.numeric(bid),
                   ask = as.numeric(ask),
                   vol.30.day.btc = as.numeric(vol.30.day.btc), 
                   vol.30.day.fiat = as.numeric(vol.30.day.fiat), 
                   vol.30.day.currency = factor(vol.30.day.currency),
                   low.30.day = as.numeric(low.30.day), 
                   high.30.day = as.numeric(high.30.day), 
                   price.24.hr = as.numeric(price.24.hr), 
                   price.24.hr.change = as.numeric(price.24.hr.change), 
                   price.24.hr.change.percent = as.numeric(price.24.hr.change.percent),
                   vol.24.hr.btc = as.numeric(vol.24.hr.btc), 
                   vol.24.hr.fiat = as.numeric(vol.24.hr.fiat), 
                   vol.24.hr.currency = factor(vol.24.hr.currency),
                   low.24.hr = as.numeric(low.24.hr), 
                   high.24.hr = as.numeric(high.24.hr))
  tbl$fiat.currency <- tbl$vol.30.day.currency
  tbl <- tbl[ ,setdiff(colnames(tbl), c('vol.24.hr.currency', 'vol.30.day.currency')) ]
  return(tbl)
}