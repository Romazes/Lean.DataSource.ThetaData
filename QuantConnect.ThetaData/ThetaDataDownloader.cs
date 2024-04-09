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
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Securities;
using System.Collections.Concurrent;
using QuantConnect.Logging;

namespace QuantConnect.Lean.DataSource.ThetaData
{
    public class ThetaDataDownloader : IDataDownloader, IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        private readonly ThetaDataProvider _historyProvider;

        /// <inheritdoc cref="MarketHoursDatabase" />
        private readonly MarketHoursDatabase _marketHoursDatabase;

        /// <summary>
        /// Initializes a new instance of the <see cref="ThetaDataDownloader"/>
        /// </summary>
        public ThetaDataDownloader()
        {
            _historyProvider = new();
            _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
        }

        public IEnumerable<BaseData>? Get(DataDownloaderGetParameters downloadParameters)
        {
            var symbol = downloadParameters.Symbol;

            var dataType = LeanData.GetDataType(downloadParameters.Resolution, downloadParameters.TickType);
            var exchangeHours = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var dataTimeZone = _marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            if (symbol.IsCanonical())
            {
                return GetCanonicalOptionHistory(
                    symbol,
                    downloadParameters.StartUtc,
                    downloadParameters.EndUtc,
                    dataType,
                    downloadParameters.Resolution,
                    exchangeHours,
                    dataTimeZone,
                    downloadParameters.TickType);
            }
            else
            {
                var historyRequest = new HistoryRequest(
                    startTimeUtc: downloadParameters.StartUtc,
                    endTimeUtc: downloadParameters.EndUtc, dataType,
                    symbol: symbol,
                    resolution: downloadParameters.Resolution,
                    exchangeHours: exchangeHours,
                    dataTimeZone: dataTimeZone,
                    fillForwardResolution: downloadParameters.Resolution,
                    includeExtendedMarketHours: true,
                    isCustomData: false,
                    dataNormalizationMode: DataNormalizationMode.Raw,
                    tickType: downloadParameters.TickType);

                var historyData = _historyProvider.GetHistory(historyRequest);

                if (historyData == null)
                {
                    return null;
                }

                return historyData;
            }
        }

        private IEnumerable<BaseData>? GetCanonicalOptionHistory(Symbol symbol, DateTime startUtc, DateTime endUtc, Type dataType,
            Resolution resolution, SecurityExchangeHours exchangeHours, DateTimeZone dataTimeZone, TickType tickType)
        {
            var optionContracts = GetOptions(symbol, startUtc, endUtc).ToList();

            Log.Debug($"OptionContract.Count: {optionContracts.Count()}");

            foreach (var option in optionContracts)
            {
                var historyRequest = new HistoryRequest(startUtc, endUtc, dataType, option, resolution, exchangeHours, dataTimeZone,
                    resolution, true, false, DataNormalizationMode.Raw, tickType);

                var historyBulkData = _historyProvider.DownloadHistoryBulkData(historyRequest);

                if (historyBulkData == null)
                {
                    continue;
                }

                foreach (var history in historyBulkData)
                {
                    yield return history;
                }
            }
        }

        protected virtual IEnumerable<Symbol> GetOptions(Symbol symbol, DateTime startUtc, DateTime endUtc)
        {
            var exchangeHours = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var blockingOptionCollection = new ConcurrentDictionary<DateTime, Symbol>();

            Parallel.ForEach(Time.EachTradeableDay(exchangeHours, startUtc.Date, endUtc.Date), tradeableDay =>
            {
                foreach (var optionByDate in _historyProvider.GetOptionChain(symbol, tradeableDay))
                {
                    blockingOptionCollection.TryAdd(optionByDate.ID.Date, optionByDate);
                }
            });

            var options = blockingOptionCollection.GetEnumerator();

            while (options.MoveNext())
            {
                yield return options.Current.Value;
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _historyProvider.DisposeSafely();
        }
    }
}