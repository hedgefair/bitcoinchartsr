\name{bitcoinchartsr}
\alias{bitcoinchartsr}
\docType{package}
\title{
Download tick data using bitcoincharts.com's free public trade data API.
}
\description{
Download tick data for all exchanges listed on bitcoincharts.com using v1 of the API. Downloaded tick data is stored as CSV files and can be aggregated into OHLCV formatted xts object or employed however the user sees fit. 
}
\details{
\tabular{ll}{
Package: \tab bitcoinchartsr\cr
Type: \tab Package\cr
Version: \tab 0.1\cr
Date: \tab 2013-11-15\cr
License: \tab GPL v3\cr
}
You can use this API to download data one exchange at a time. For example: the following code will download tick data for the cavirtex.com exchange (currency symbol: virtexCAD) to the "data" directory in CSV format. The CSV file will then be parsed and an xts OHLCV object (includes field called "Ticks", which is equal to number of trades executed during the period) with one-minute periodicity will be returned.

\code{virtexCAD <- get_bitcoinchartsr_data(symbol='virtexCAD', data.directory='data', ohlc.frequency='minutes', download.data=TRUE)\cr
chartSeries(virtexCAD[Sys.Date()])
}

Or you can download a single data dump and use it later...

\code{download_daily_dump(symbol='virtexCAD', data.directory='data', overwrite=TRUE)\cr
virtexCAD <- get_bitcoinchartsr_data(symbol='virtexCAD', data.directory='data', ohlc.frequency='minutes', download.data=FALSE)
}

Alternately, you can download everything in one shot and then load the data you need on a case-by-case basis:

\code{download_all_daily_dumps(base.dir='data', overwrite=TRUE)\cr
mtgoxUSD <- get_bitcoinchartsr_data(symbol='mtgoxUSD', data.directory='data/mtgoxUSD', ohlc.frequency='hourly', download.data=TRUE)
}

}
\author{
Peter Taylor

Maintainer: Dev Team <helpdesk@taypeinternational.com>
}
\references{
Please see \code{\link[http://bitcoincharts.com/about/markets-api/]{http://bitcoincharts.com/about/markets-api}} for full information on the Markets API
}
\keyword{ package }
\seealso{
\code{\link[Quandl:Quandl]{Quandl}} also provides a very handy, R-friendly API to obtain lower frequency Bitcoin market data:
}
\examples{
\code{library(bitcoinchartsr)\cr
get_bitcoinchartsr_data(symbol='mtgoxUSD', data.directory='inst//data', start.date='2013-01-01', ohlc.frequency='days', auto.assign=TRUE, download.data=F)\cr
# let's look at busy times of week at MtGox
barplot(tapply(mtgoxUSD$Ticks, INDEX=as.factor(weekdays(index(mtgoxUSD))), median)[c('Sunday','Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday')], las=2, ylab='Median count of trades per day', main='Median count of USD trades per day at MtGox')\cr
}
}