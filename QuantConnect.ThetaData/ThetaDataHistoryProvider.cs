/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using NodaTime;
using RestSharp;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Logging;
using QuantConnect.Interfaces;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Lean.Engine.HistoricalData;
using QuantConnect.Lean.DataSource.ThetaData.Models.Rest;
using QuantConnect.Lean.DataSource.ThetaData.Models.Common;
using QuantConnect.Lean.DataSource.ThetaData.Models.Interfaces;

namespace QuantConnect.Lean.DataSource.ThetaData
{
    /// <summary>
    /// ThetaData.net implementation of <see cref="IHistoryProvider"/>
    /// </summary>
    public partial class ThetaDataProvider : SynchronizingHistoryProvider
    {
        /// <summary>
        /// Indicates whether the warning for invalid <see cref="SecurityType"/> has been fired.
        /// </summary>
        private volatile bool _invalidSecurityTypeWarningFired;

        /// <summary>
        /// Indicates whether the warning for invalid <see cref="ISubscriptionPlan.AccessibleResolutions"/> has been fired.
        /// </summary>
        private volatile bool _invalidSubscriptionResolutionRequestWarningFired;

        /// <summary>
        /// Indicates whether the warning indicating that the requested date is greater than the <see cref="ISubscriptionPlan.FirstAccessDate"/> has been triggered.
        /// </summary>
        private volatile bool _invalidStartDateInCurrentSubscriptionWarningFired;

        /// <summary>
        /// Indicates whether a warning for an invalid start time has been fired, where the start time is greater than or equal to the end time in UTC.
        /// </summary>
        private volatile bool _invalidStartTimeWarningFired;

        /// <summary>
        /// Indicates whether an warning should be raised when encountering invalid open interest data for an option security type at daily resolution.
        /// </summary>
        /// <remarks>
        /// This flag is set to true when an error is detected for invalid open interest data for options at daily resolution.
        /// </remarks>
        private volatile bool _invalidOpenInterestWarningFired;

        /// <inheritdoc />
        public override void Initialize(HistoryProviderInitializeParameters parameters)
        { }

        /// <inheritdoc />
        public override IEnumerable<Slice>? GetHistory(IEnumerable<HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            var subscriptions = new List<Subscription>();
            foreach (var request in requests)
            {
                var history = GetHistory(request);

                var subscription = CreateSubscription(request, history);
                if (!subscription.MoveNext())
                {
                    continue;
                }

                subscriptions.Add(subscription);
            }

            if (subscriptions.Count == 0)
            {
                return null;
            }
            return CreateSliceEnumerableFromSubscriptions(subscriptions, sliceTimeZone);
        }

        public IEnumerable<BaseData>? GetHistory(HistoryRequest historyRequest)
        {
            if (!ValidateHistoryRequest(historyRequest))
            {
                return null;
            }

            var restRequest = new RestRequest(Method.GET);

            var startDate = historyRequest.StartTimeUtc.ConvertFromUtc(TimeZones.EasternStandard).ConvertToThetaDataDateFormat();
            var endDate = historyRequest.EndTimeUtc.ConvertFromUtc(TimeZones.EasternStandard).ConvertToThetaDataDateFormat();

            restRequest.AddQueryParameter("start_date", startDate);
            restRequest.AddQueryParameter("end_date", endDate);

            switch (historyRequest.Symbol.SecurityType)
            {
                case SecurityType.Option:
                    return GetOptionHistoryData(restRequest, historyRequest.Symbol, historyRequest.Resolution, historyRequest.TickType);
            }

            return null;
        }

        public bool ValidateHistoryRequest(HistoryRequest historyRequest)
        {
            if (!_userSubscriptionPlan.AccessibleResolutions.Contains(historyRequest.Resolution))
            {
                if (!_invalidSubscriptionResolutionRequestWarningFired)
                {
                    _invalidSubscriptionResolutionRequestWarningFired = true;
                    Log.Trace($"{nameof(ThetaDataProvider)}.{nameof(ValidateHistoryRequest)}: The current user's subscription plan does not support the requested resolution: {historyRequest.Resolution}");
                }
                return false;
            }

            if (_userSubscriptionPlan.FirstAccessDate.Date > historyRequest.StartTimeUtc.Date)
            {
                if (!_invalidStartDateInCurrentSubscriptionWarningFired)
                {
                    _invalidStartDateInCurrentSubscriptionWarningFired = true;
                    Log.Trace($"{nameof(ThetaDataProvider)}.{nameof(ValidateHistoryRequest)}: The requested start time ({historyRequest.StartTimeUtc.Date}) exceeds the maximum available date ({_userSubscriptionPlan.FirstAccessDate.Date}) allowed by the user's subscription.");
                }
            }

            if (!CanSubscribe(historyRequest.Symbol))
            {
                if (!_invalidSecurityTypeWarningFired)
                {
                    _invalidSecurityTypeWarningFired = true;
                    Log.Trace($"{nameof(ThetaDataProvider)}.{nameof(ValidateHistoryRequest)}: Unsupported SecurityType '{historyRequest.Symbol.SecurityType}' for symbol '{historyRequest.Symbol}'");
                }
                return false;
            }

            if (historyRequest.StartTimeUtc >= historyRequest.EndTimeUtc)
            {
                if (!_invalidStartTimeWarningFired)
                {
                    _invalidStartTimeWarningFired = true;
                    Log.Error($"{nameof(ThetaDataProvider)}.{nameof(ValidateHistoryRequest)}: Error - The start date in the history request must come before the end date. No historical data will be returned.");
                }
                return false;
            }

            if (historyRequest.Symbol.SecurityType == SecurityType.Option && historyRequest.TickType == TickType.OpenInterest && historyRequest.Resolution != Resolution.Daily)
            {
                if (!_invalidOpenInterestWarningFired)
                {
                    _invalidOpenInterestWarningFired = true;
                    Log.Trace($"Invalid data request: TickType 'OpenInterest' only supports Resolution 'Daily' and SecurityType 'Option'. Requested: Resolution '{historyRequest.Resolution}', SecurityType '{historyRequest.Symbol.SecurityType}'.");
                }
                return false;
            }

            return true;
        }

        public IEnumerable<BaseData>? DownloadHistoryBulkData(HistoryRequest historyRequest)
        {
            if (!ValidateHistoryRequest(historyRequest))
            {
                return null;
            }

            switch (historyRequest.Symbol.SecurityType)
            {
                case SecurityType.Option:
                    return GetOptionBulkHistoryData(historyRequest.Symbol, historyRequest.Resolution, historyRequest.StartTimeUtc, historyRequest.EndTimeUtc);
                default:
                    throw new NotSupportedException($"{nameof(ThetaDataProvider)}.{nameof(DownloadHistoryBulkData)}: Unsupported security type '{historyRequest.Symbol.SecurityType}'");
            }
        }

        public static IEnumerable<(DateTime startDate, DateTime endDate)> SplitDateRange(DateTime start, DateTime end, int dayChunkSize)
        {
            DateTime chunkEnd;
            while ((chunkEnd = start.AddDays(dayChunkSize)) < end)
            {
                yield return (start, chunkEnd);
                start = chunkEnd;
            }
            yield return (start, end);
        }

        private IEnumerable<BaseData> GetOptionBulkHistoryData(Symbol optionSymbol, Resolution resolution, DateTime startDateUtc, DateTime endDateUtc)
        {
            var optionRestRequest = new RestRequest("/bulk_hist/option/quote", Method.GET);

            var ticker = _symbolMapper.GetBrokerageSymbol(optionSymbol).Split(',');

            optionRestRequest.AddQueryParameter("root", ticker[0]);
            optionRestRequest.AddQueryParameter("exp", ticker[1]);
            optionRestRequest.AddQueryParameter("ivl", GetIntervalsInMilliseconds(resolution));

            var optionsSecurityType = optionSymbol.SecurityType == SecurityType.Index ? SecurityType.IndexOption : SecurityType.Option;
            var optionStyle = optionsSecurityType.DefaultOptionStyle();
            var period = resolution.ToTimeSpan();

            foreach (var dates in SplitDateRange(startDateUtc, endDateUtc, 5))
            {
                optionRestRequest.AddOrUpdateParameter("start_date", dates.startDate.ConvertFromUtc(TimeZones.EasternStandard).ConvertToThetaDataDateFormat());
                optionRestRequest.AddOrUpdateParameter("end_date", dates.endDate.ConvertFromUtc(TimeZones.EasternStandard).ConvertToThetaDataDateFormat());

                foreach (var bulkQuote in _restApiClient.ExecuteRequest<BaseResponse<BulkQuoteResponse>>(optionRestRequest))
                {
                    foreach (var quotes in bulkQuote.Response)
                    {
                        var contract = _symbolMapper.GetLeanSymbol(quotes.Contract.Root, optionsSecurityType, optionSymbol.ID.Market, optionStyle,
                        quotes.Contract.ExpirationDate, quotes.Contract.Strike, quotes.Contract.Right == "C" ? OptionRight.Call : OptionRight.Put, optionSymbol.Underlying);

                        foreach (var quote in quotes.Quotes)
                        {
                            // If Ask/Bid - prices/sizes zero, low quote activity, empty result, low volatility.
                            if (quote.AskPrice == 0 || quote.AskSize == 0 || quote.BidPrice == 0 || quote.BidSize == 0)
                            {
                                continue;
                            }

                            var bar = new QuoteBar(quote.DateTimeMilliseconds, contract, null, decimal.Zero, null, decimal.Zero, period);
                            bar.UpdateQuote(quote.BidPrice, quote.BidSize, quote.AskPrice, quote.AskSize);
                            yield return bar;
                        }
                    }
                }
            }
        }

        public IEnumerable<BaseData>? GetOptionHistoryData(RestRequest optionRequest, Symbol symbol, Resolution resolution, TickType tickType)
        {
            var ticker = _symbolMapper.GetBrokerageSymbol(symbol).Split(',');

            optionRequest.AddQueryParameter("root", ticker[0]);
            optionRequest.AddQueryParameter("exp", ticker[1]);
            optionRequest.AddQueryParameter("strike", ticker[2]);
            optionRequest.AddQueryParameter("right", ticker[3]);

            if (resolution == Resolution.Daily)
            {
                switch (tickType)
                {
                    case TickType.Trade:
                        optionRequest.Resource = "/hist/option/eod";
                        var period = resolution.ToTimeSpan();
                        return GetOptionEndOfDay(optionRequest,
                            // If OHLC prices zero, low trading activity, empty result, low volatility.
                            (eof) => eof.Open == 0 || eof.High == 0 || eof.Low == 0 || eof.Close == 0,
                            (tradeDateTime, eof) => new TradeBar(tradeDateTime, symbol, eof.Open, eof.High, eof.Low, eof.Close, eof.Volume, period));
                    case TickType.Quote:
                        optionRequest.Resource = "/hist/option/eod";
                        return GetOptionEndOfDay(optionRequest,
                            // If Ask/Bid - prices/sizes zero, low quote activity, empty result, low volatility.
                            (eof) => eof.AskPrice == 0 || eof.AskSize == 0 || eof.BidPrice == 0 || eof.BidSize == 0,
                            (quoteDateTime, eof) =>
                            {
                                var bar = new QuoteBar(quoteDateTime, symbol, null, decimal.Zero, null, decimal.Zero, resolution.ToTimeSpan());
                                bar.UpdateQuote(eof.BidPrice, eof.BidSize, eof.AskPrice, eof.AskSize);
                                return bar;
                            });
                    case TickType.OpenInterest:
                        optionRequest.Resource = "/hist/option/open_interest";
                        return GetHistoricalOpenInterestData(optionRequest, symbol);
                    default:
                        throw new ArgumentException($"Invalid tick type: {tickType}.");
                }
            }
            else
            {
                switch (tickType)
                {
                    case TickType.Trade:
                        optionRequest.Resource = "/hist/option/trade";
                        var tickTradeBars = GetHistoricalTickTradeData(optionRequest, symbol);
                        if (resolution != Resolution.Tick)
                        {
                            return LeanData.AggregateTicksToTradeBars(tickTradeBars, symbol, resolution.ToTimeSpan());
                        }
                        return tickTradeBars;
                    case TickType.Quote:
                        optionRequest.AddQueryParameter("ivl", GetIntervalsInMilliseconds(resolution));
                        optionRequest.Resource = "/hist/option/quote";

                        Func<QuoteResponse, BaseData> quoteCallback = resolution == Resolution.Tick ?
                            (quote) => new Tick(quote.DateTimeMilliseconds, symbol, quote.AskCondition, ThetaDataExtensions.Exchanges[quote.AskExchange], quote.BidSize, quote.BidPrice, quote.AskSize, quote.AskPrice)
                            :
                            (quote) =>
                            {
                                var bar = new QuoteBar(quote.DateTimeMilliseconds, symbol, null, decimal.Zero, null, decimal.Zero, resolution.ToTimeSpan());
                                bar.UpdateQuote(quote.BidPrice, quote.BidSize, quote.AskPrice, quote.AskSize);
                                return bar;
                            };

                        return GetHistoricalQuoteData(optionRequest, symbol, quoteCallback);
                    default:
                        throw new ArgumentException($"Invalid tick type: {tickType}.");
                }
            }
        }

        private IEnumerable<BaseData> GetHistoricalOpenInterestData(RestRequest request, Symbol symbol)
        {
            foreach (var openInterests in _restApiClient.ExecuteRequest<BaseResponse<OpenInterestResponse>>(request))
            {
                foreach (var openInterest in openInterests.Response)
                {
                    yield return new OpenInterest(openInterest.DateTimeMilliseconds, symbol, openInterest.OpenInterest);
                }
            }
        }

        private IEnumerable<Tick> GetHistoricalTickTradeData(RestRequest request, Symbol symbol)
        {
            foreach (var trades in _restApiClient.ExecuteRequest<BaseResponse<TradeResponse>>(request))
            {
                foreach (var trade in trades.Response)
                {
                    yield return new Tick(trade.DateTimeMilliseconds, symbol, trade.Condition.ToStringInvariant(), ThetaDataExtensions.Exchanges[trade.Exchange], trade.Size, trade.Price);
                }
            }
        }

        private IEnumerable<BaseData> GetHistoricalQuoteData(RestRequest request, Symbol symbol, Func<QuoteResponse, BaseData> callback)
        {
            foreach (var quotes in _restApiClient.ExecuteRequest<BaseResponse<QuoteResponse>>(request))
            {
                foreach (var quote in quotes.Response)
                {
                    // If Ask/Bid - prices/sizes zero, low quote activity, empty result, low volatility.
                    if (quote.AskPrice == 0 || quote.AskSize == 0 || quote.BidPrice == 0 || quote.BidSize == 0)
                    {
                        continue;
                    }

                    yield return callback(quote);
                }
            }
        }

        private IEnumerable<BaseData>? GetOptionEndOfDay(RestRequest request, Func<EndOfDayReportResponse, bool> validateEmptyResponse, Func<DateTime, EndOfDayReportResponse, BaseData> res)
        {
            foreach (var endOfDays in _restApiClient.ExecuteRequest<BaseResponse<EndOfDayReportResponse>>(request))
            {
                foreach (var endOfDay in endOfDays.Response)
                {
                    if (validateEmptyResponse(endOfDay))
                    {
                        continue;
                    }
                    yield return res(endOfDay.LastTradeDateTimeMilliseconds, endOfDay);
                }
            }
        }

        private IEnumerable<BaseData>? GetHistoricalOptionBulkQuoteData(Symbol underlying, TimeSpan resolution)
        {
            var request2 = new RestRequest("http://127.0.0.1:25510/v2/bulk_hist/option/quote", Method.GET);

            request2.AddQueryParameter("root", "NVDA");
            request2.AddQueryParameter("ivl", "60000");
            request2.AddQueryParameter("exp", "20240426");
            request2.AddQueryParameter("start_date", "20240301");
            request2.AddQueryParameter("end_date", "20240401");

            var optionsSecurityType = underlying.SecurityType == SecurityType.Index ? SecurityType.IndexOption : SecurityType.Option;
            var optionStyle = optionsSecurityType.DefaultOptionStyle();

            foreach (var bulkQuote in _restApiClient.ExecuteRequest<BaseResponse<BulkQuoteResponse>>(request2))
            {
                foreach (var quotes in bulkQuote.Response)
                {
                    var contract = _symbolMapper.GetLeanSymbol(quotes.Contract.Root, optionsSecurityType, underlying.ID.Market, optionStyle,
                    quotes.Contract.ExpirationDate, quotes.Contract.Strike, quotes.Contract.Right == "C" ? OptionRight.Call : OptionRight.Put, underlying);

                    foreach (var quote in quotes.Quotes)
                    {
                        var bar = new QuoteBar(quote.DateTimeMilliseconds, contract, null, decimal.Zero, null, decimal.Zero, resolution);
                        bar.UpdateQuote(quote.BidPrice, quote.BidSize, quote.AskPrice, quote.AskSize);
                        yield return bar;
                    }
                }
            }
        }

        /// <summary>
        /// Returns the interval in milliseconds corresponding to the specified resolution.
        /// </summary>
        /// <param name="resolution">The <see cref="Resolution"/> for which to retrieve the interval.</param>
        /// <returns>
        /// The interval in milliseconds as a string. 
        /// For <see cref="Resolution.Tick"/>, returns "0".
        /// For <see cref="Resolution.Second"/>, returns "1000".
        /// For <see cref="Resolution.Minute"/>, returns "60000".
        /// For <see cref="Resolution.Hour"/>, returns "3600000".
        /// </returns>
        /// <exception cref="NotSupportedException">Thrown when the specified resolution is not supported.</exception>
        private string GetIntervalsInMilliseconds(Resolution resolution) => resolution switch
        {
            Resolution.Tick => "0",
            Resolution.Second => "1000",
            Resolution.Minute => "60000",
            Resolution.Hour => "3600000",
            _ => throw new NotSupportedException($"The resolution type '{resolution}' is not supported.")
        };
    }
}