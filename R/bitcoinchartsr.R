#' @title Download single exchange data file from bitcoincharts.com API
#' @description Download single exchange data file from bitcoincharts.com API
#' @param symbol character. Supported exchanges can be obtained by calling the 
#' \code{'get_symbol_listing()'} method.
#' @param data.directory character. Destination directory for downloaded data files. 
#' Defaults to package install extdata/marketdata for the symbol specified directory.
#' @param overwrite logical. Whether to overwrite the local copy of the data file.
#' @param debug logical. Debugging flag.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://api.bitcoincharts.com/v1/csv/}}
#' @export
#' @examples
#' \dontrun{
#' # Get the full dump of all MTGOX USD transactions
#' download_data_file(symbol='mtgoxUSD', data.directory='/home/user/Desktop')
#' }
#' @import stringr httr R.utils
download_data_file <- function(symbol, 
                               data.directory = file.path(system.file('extdata', 
                                                                      'market-data', 
                                                                      mustWork = TRUE, 
                                                                      package = 'bitcoinchartsr'), 
                                                          symbol), 
                               overwrite = TRUE) {
  file.name <- file.path(data.directory, paste0(symbol, '-dump.csv.gz'))
  if(!file.exists(data.directory)) {
    dir.create(data.directory)
  } 
  if(file.exists(file.name)) {
    if(overwrite) unlink(file.name) else return(file.name)
  }
  url <- paste0('http://api.bitcoincharts.com/v1/csv/', symbol, '.csv.gz')
  message(url)
  res <- GET(url, verbose(), progress(), write_disk(file.name, overwrite = TRUE))
  stop_for_status(res)
  message('downloaded')
  # now gunzip the file
  new.file.name <- str_replace(file.name, '.gz', '')
  gunzip(filename = file.name, destname = new.file.name, overwrite=TRUE)
  return(new.file.name)
}

#' @title Download all available archived trade data
#' @description Download all available data for all markets in one fell swoop. 
#' Caution! Some exchange data files are > 500 MB!!!
#' @param data.directory character. Destination directory for downloaded 
#' data files. Defaults to package install extdata/marketdata directory.
#' @param overwrite logical. Whether to overwrite the local copy of the data file.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://api.bitcoincharts.com/v1/csv/}}
#' @export
#' @import httr XML R.utils
#' @examples
#' \dontrun{
#' # Download all available market data in one fell swoop:
#' downdownload_all_data_files()
#' }
download_all_data_files <- function(data.directory = system.file('extdata', 
                                                                 'market-data', 
                                                                 mustWork = TRUE, 
                                                                 package = 'bitcoinchartsr'), 
                                    overwrite = TRUE) {
  # get all csv download links from http://api.bitcoincharts.com/v1/csv/
  url <- 'http://api.bitcoincharts.com/v1/csv/'
  pg <- GET(url)
  stop_for_status(pg)
  pg <- content(pg)
  files <- setdiff(unlist(lapply(xpathApply(pg, '//a'), xmlGetAttr, 'href')), '../')
  urls <- paste0(url, files)
  # download the files if needed
  file.names <- lapply(sample(urls), function(url) {
    message(url)
    dir.name <- file.path(data.directory, str_replace_all(basename(url), '\\.csv\\.gz', ''))
    file.name <- file.path(dir.name, basename(url))
    # if data dir doesn't exist create it
    if(!file.exists(dir.name)) dir.create(dir.name, showWarnings = TRUE)
    if(overwrite | !exists(file.name)) {
      res <- GET(url, verbose(), progress(), write_disk(file.name, overwrite = TRUE))
      stop_for_status(res)
      message('downloaded')
      Sys.sleep(1)
      # now gunzip the file
      new.file.name <- str_replace(file.name, '.gz', '')
      gunzip(filename = file.name, destname = new.file.name, overwrite=TRUE)
      return(new.file.name)
    } else {
      message('skipped')
      return(NA)
    }
  })
  file.names <- unlist(file.names)
  file.names
}

#' @title get_ticks_from_file
#' @name get_ticks_from_file
#' @description Download all historical trade data for a given symbol from 
#' bitcoincharts.com. Then parse the files and return tick data for the specified 
#' time period
#' @param symbol bitcoincharts symbol to get trade data from file for
#' @param start.date defaults to 1 day before the current time
#' @param end.date deafults to current time
#' @param data.directory directory for data file
#' @import lubridate xts 
#' @importFrom data.table fread
#' @export
get_ticks_from_file <- function(symbol, 
                                start.date = as.character(now() - hours(6)), 
                                end.date = as.character(now()), 
                                data.directory = file.path(system.file('extdata', 
                                                                       'market-data', 
                                                                       mustWork = TRUE, 
                                                                       package = 'bitcoinchartsr'), 
                                                           symbol)) 
  
#' @title get_ticks_from_multiple_files
#' @name get_ticks_from_multiple_files
#' @description load data for all known symbols into the global environment
#' @param start.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format 
#' representing the start of the requested data series. Defaults to 30 days prior 
#' to current date.
#' @param symbols symbols to load ticks for
#' @param end.date character. Character string in YYYY-MM-DD (%Y-%m-%d) format 
#' representing the start of the requested data series. Defaults to current date + 1.
#' @param data.directory character. Destination directory for downloaded data 
#' files. Defaults to package install extdata/marketdata directory.
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @examples
#' \dontrun{
#' Load all data available up to the current minuute on bitcoincharts.com into 
#' the global environment
#' get_ticks_from_multiple_files()
#' }
#' @import lubridate xts zoo
#' @export
get_ticks_from_multiple_files <- function(symbols = get_symbol_listing(),
                                 start.date = as.character(now() - hours(6)), 
                                 end.date = as.character(now()), 
                                 data.directory = system.file('extdata', 
                                                              'market-data', 
                                                              mustWork = TRUE, 
                                                              package = 'bitcoinchartsr')) {
  # load all ticks from file
  ticks <- lapply(symbols, function(x) {
    get_ticks_from_file(symbol = x, 
                        data.directory = data.directory, 
                        start.date = start.date, 
                        end.date = end.date)
  })
  # convert data.frames to xts
  ticks <- lapply(ticks, function(x) {
    xts(x[ , -1 ], order.by = x[ , 1 ])
  })
  names(ticks) <- symbols
  # jam them all together into a massive xts object
  ticks <- do.call('cbind', ticks)
  ticks
}

#' get_ticks_from_api
#' @title get_ticks_from_api
#' @description get_ticks_from_api
#' @name get_ticks_from_api
#' @param symbol 
#' @param start.date
#' @param end.date
#' @return data.frame with latest ticks from api
#' @import httr xts
#' @export
get_ticks_from_api <- function(symbol,
                               start.date = as.character(now() - hours(6)),
                               end.date = as.character(now())) {
  start <- as.integer(as.POSIXct(start.date))
  url <- paste0('http://api.bitcoincharts.com/v1/trades.csv?symbol=', symbol, '&start=', start)
  ticks <- GET(url, verbose())
  stop_for_status(ticks)
  ticks <- content(ticks, as = 'text')
  ticks <- read.csv(textConnection(ticks), header = FALSE)
  ticks <- xts(ticks[ , 2:3 ], 
               order.by = as.POSIXct(ticks[ , 1 ], origin = '1970-01-01'))
  colnames(ticks) <- c('price', 'amount')
  ticks <- ticks[ paste0('::', end.date) ]
  ticks
}

#' @title to_ohlc_xts
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
to_ohlc_xts <- function(ttime, 
                        tprice, 
                        tvolume, 
                        fmt, 
                        align = FALSE,  
                        fill = FALSE) {
  if(fill) align <- TRUE
  ttime.int <- format(ttime, fmt)
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
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @export
#' @examples
#' \dontrun{
#' # Get one month of hourly market data for virtexCAD:
#' get_ohlcv_from_file('virtexCAD')
#' }
#' @import lubridate stringr xts zoo
get_ohlcv_from_file <- function(symbol, 
                                start.date = as.character(now() - hours(6)), 
                                end.date = as.character(now()), 
                                ohlc.frequency = 'hours', 
                                align = TRUE,
                                fill = FALSE,
                                data.directory = file.path(system.file('extdata', 
                                                                       'market-data', 
                                                                       mustWork = TRUE, 
                                                                       package = 'bitcoinchartsr'), 
                                                           symbol), 
                                download.data = FALSE, 
                                overwrite = FALSE) {
  # sanity checks
  stopifnot(ohlc.frequency %in% c('seconds', 'minutes', 'hours', 'days', 'months', 'years'))
  # get the format str for ohlc transformation
  if(ohlc.frequency == 'seconds') formatstr <- '%Y%m%d %H %M %S'
  if(ohlc.frequency == 'minutes') formatstr <- '%Y%m%d %H %M'
  if(ohlc.frequency == 'hours') formatstr <- '%Y%m%d %H'
  if(ohlc.frequency == 'days') formatstr <- '%Y%m%d'
  if(ohlc.frequency == 'months') formatstr <- '%Y%m'
  if(ohlc.frequency == 'years') formatstr <- '%Y'
  # get tick data for symbol
  ticks <- get_ticks_from_file(symbol = symbol, 
                               data.directory = data.directory, 
                               start.date = start.date, 
                               end.date = end.date)
  if(nrow(ticks) == 0) return(NA)
  # aggregate tick data to ohlcv format
  ohlc.data.xts <- to_ohlc_xts(ttime = index(ticks), 
                               tprice = as.numeric(ticks$price), 
                               tvolume = as.numeric(ticks$amount), 
                               fmt = formatstr, 
                               align = align, 
                               fill = fill)    
  ohlc.data.xts <- setNames(ohlc.data.xts, 
                            c('Open', 'High', 'Low', 'Close', 'Volume', 'Ticks'))
  return(ohlc.data.xts)  
}

#' @title Get OHLC for last two days of trades via API
#' @description Utility function which returns the most recent OHLC candle from 
#' the bitcoincharts.com API
#' @param symbol character. Supported exchanges can be obtained by calling the 
#' \code{'get_symbol_listing()'} method.
#' @param start.date
#' @param end.date
#' @param data.directory character. Destination directory for downloaded data 
#' files. Defaults to package install extdata/marketdata directory.
#' @param ohlc.frequency character. Supported values are \code{seconds}, 
#' \code{minutes}, \code{hours}, \code{days}, \code{months}, \code{years}
#' @param align logical. Align time series index.
#' @param fill logical. Fill missing values. 
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://api.bitcoincharts.com/v1/trades.csv?symbol=SYMBOL[&end=UNIXTIME]}}
#' @export
#' @examples
#' \dontrun{
#' # Get most recent 1-day candle from cavirtex.com:
#' get_most_recent_trade('virtexCAD', ohlc.frequency='days')
#' }
#' @import lubridate zoo xts
get_ohlcv_from_api <- function(symbol, 
                               start.date = as.character(now() - hours(6)),
                               end.date = as.character(now()),
                               data.directory = system.file('extdata', 
                                                            'market-data', 
                                                            mustWork = TRUE, 
                                                            package = 'bitcoinchartsr'), 
                               ohlc.frequency = 'hours', 
                               align = TRUE,
                               fill = FALSE) {
  # sanity checks
  stopifnot(ohlc.frequency %in% c('seconds', 'minutes', 'hours', 'days', 'months', 'years'))
  # get the format str for ohlc transformation
  if(ohlc.frequency == 'seconds') formatstr <- '%Y%m%d %H %M %S'
  if(ohlc.frequency == 'minutes') formatstr <- '%Y%m%d %H %M'
  if(ohlc.frequency == 'hours') formatstr <- '%Y%m%d %H'
  if(ohlc.frequency == 'days') formatstr <- '%Y%m%d'
  if(ohlc.frequency == 'months') formatstr <- '%Y%m'
  if(ohlc.frequency == 'years') formatstr <- '%Y'
  # get tick data from api
  ticks <- get_ticks_from_api(symbol = symbol, start = start.date)
  if(nrow(ticks) == 0) return(NA)
  # aggregate tick data to ohlcv format
  ohlc.data.xts <- to_ohlc_xts(ttime = index(ticks), 
                               tprice = as.numeric(ticks$price), 
                               tvolume = as.numeric(ticks$amount), 
                               fmt = formatstr, 
                               align = align, 
                               fill = fill)    
  ohlc.data.xts <- setNames(ohlc.data.xts, 
                            c('Open', 'High', 'Low', 'Close', 'Volume', 'Ticks'))
  ohlc.data.xts
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
#' @import httr XML
get_symbol_listing <- function(debug = FALSE) {
  exchanges <- content(GET('http://bitcoincharts.com/markets/'))
  exchanges <- unlist(lapply(xpathApply(exchanges, '//span[ @class = "sub" ]//a'), xmlValue))
  exchanges
}

#' @title Get detailed information for a specified exchange
#' @description Get detailed listing of all currently available exchanges from 
#' http://bitcoincharts.com/markets
#' @param symbol character. Exchange you wish to obtain info for
#' @references \url{http://bitcoincharts.com/about/markets-api/}
#' @seealso \code{\link{http://bitcoincharts.com/markets}}
#' @export
#' @examples
#' \dontrun{
#' # Get all market symbols:
#' get_exchange_info()
#' }
#' @import XML httr
get_exchange_info <- function(symbol) {
  markets.url <- 'http://bitcoincharts.com/markets/'
  markets.url <- paste0(markets.url, '/', symbol, '.html')
  pg <- GET(markets.url)
  stop_for_status(pg)
  pg <- content(pg)
  labels <- xmlApply(xpathApply(pg, "//div//div//p//label"), xmlValue)
  vals <- xmlApply(xpathApply(pg, "//div//div//p//span"), xmlValue)
  df <- data.frame(cbind(key = labels, value = vals), stringsAsFactors = FALSE)
  df
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
#' @import XML httr
get_total_mined_coins <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  pg <- GET(markets.url)
  stop_for_status(pg)
  pg <- content(pg)
  tbl <- readHTMLTable(pg, header = FALSE)[[ 1 ]]
  colnames(tbl) <-c('key', 'value')
  tbl
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
#' @import XML httr
get_current_difficulty <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  pg <- GET(markets.url)
  stop_for_status(pg)
  pg <- content(pg)
  tbl <- readHTMLTable(pg, header = FALSE)[[ 2 ]]
  colnames(tbl) <-c('key', 'value')
  tbl
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
#' @import XML httr
get_total_network_hashing_power <- function() {
  markets.url = 'http://bitcoincharts.com/markets/'
  pg <- GET(markets.url)
  stop_for_status(pg)
  pg <- content(pg)
  tbl <- readHTMLTable(pg, header = FALSE)[[ 3 ]]
  colnames(tbl) <-c('key', 'value')
  tbl
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
  stop('Broken!')
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
