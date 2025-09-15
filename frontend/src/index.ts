import { DateTime } from 'luxon';
import Plotly from 'plotly.js-dist';
import './styles.css';
// URLs
const getStockURL: string = "http://127.0.0.1:7000/stock";
const getStockStatURL: string = "http://127.0.0.1:7000/stock/stat";
const getKeywordURL: string = "http://127.0.0.1:7000/keyword";
const getKeywordStatURL: string = "http://127.0.0.1:7000/keyword/stat";
// HTML elements
const stockTileDiv = document.getElementById("stock-tile");
const stockDateBtns = document.querySelectorAll(".stock-date-btn");
const kwTileDiv = document.getElementById("kw-tile");
const kwDateBtns = document.querySelectorAll(".kw-date-btn");
// JS variables
const oneYearAgo = DateTime.now().minus({'years': 1}).toISODate();
let firstTime = true
// Colors
// Plot palette
const DARKBLUE = 'rgb(1,35,97)';
const MIDBLUE = 'rgb(44,101,198)';
const LIGHTBLUE = 'rgb(4,191,191)';
const LAVENDER = 'rgb(194,209,238)';
// Icons palette
const UPGREEN = '#328E6E';
const DOWNRED = '#B9375D';
const MIDYELLOW = '#CDC2A5';

enum STOCK_INTERVAL {
    RANGE_3M = 'stock3m',
    RANGE_6M = 'stock6m',
    RANGE_1Y = 'stock1y',
    RANGE_2Y = 'stock2y',
}
enum KW_INTERVAL {
    RANGE_3M = 'kw3m',
    RANGE_6M = 'kw6m',
    RANGE_1Y = 'kw1y',
    RANGE_2Y = 'kw2y',

}

interface TrendSeries {
    stock_date: string[],
    open: number[],
    close: number[],
    low: number[],
    high: number[],
}

const getStartDate = (btnId: string, enumRange: typeof STOCK_INTERVAL| typeof KW_INTERVAL) => {
    switch(btnId) {
        case enumRange.RANGE_3M:
            return DateTime.now().minus({'months': 3}).toISODate()
        case enumRange.RANGE_6M:
            return DateTime.now().minus({'months': 6}).toISODate()
        case enumRange.RANGE_1Y:
            return DateTime.now().minus({'years': 1}).toISODate()
        case enumRange.RANGE_2Y:
            return DateTime.now().minus({'years': 2}).toISODate()
    }
}

const queryBuilder = (startDate:string, url: string) => {
    const endDate = DateTime.now().toISODate();
    const queryString = encodeURI(`?start_date="${startDate}"&end_date="${endDate}"`);
    return url + queryString;
}
// Get trend values
const fetchTrends = async(startDate:string = oneYearAgo, URL: string) => {
    const URLparams = queryBuilder(startDate, URL)
    try {
        const response = await fetch(URLparams);
        if(response.status != 200) {
            throw new Error(`Response status: ${response.status}`);
        };
        const data = JSON.parse(await response.json());
        return data
    }
    catch ( error: any ) {
        console.log(error.message);
    }
}
// Get stats value
const fetchStats = async( URL: string ) => {
    try {
        const response = await fetch(URL);
        if (response.status != 200) {
            throw new Error(`Response status: ${response.status}`);
        };
        const data = JSON.parse(await response.json());
        return data
    }
    catch ( error: any ) {
        console.log(error.message);
    }
}

const displayStockStats = ( data: any ) => {
    // Round number features
    for (const key in data) {
        if ( typeof(data[key]) === "number" ) {
            data[key].toFixed(2);
        };
    };
    // Create and append content to card
    const appendCardContent = ( statDiv: HTMLDivElement, percentage: string, featureName: string ) => {
        try {
            // Create title for mini tile
            const cardTitle = document.createElement("div");
            cardTitle.className = "card-title";
            cardTitle.append( `${featureName[0].toUpperCase()}${featureName.slice(1)}` );
            cardTitle.className = "card-title";
            // Create first line
            const firstLineDiv = document.createElement("div");
            firstLineDiv.className = "tile-first-line";
            // Elements of fist line
            const arrowImg = document.createElement("i");
            const coloredPercentage = document.createElement("div");
            let className, classCard, color;
            // Create second line
            const secondLineDiv = document.createElement("div")
            secondLineDiv.className = "tile-second-line";
            secondLineDiv.textContent = `Value: ${data[featureName]} EUR`;
            // Create wrapper div for body card
            const bodyCard = document.createElement("div");
            bodyCard.className = "body-card";

            if ( data[percentage] > 0.1 ) {
                className = "fa fa-arrow-up";
                classCard = "positive";
                color = UPGREEN;
            }
            else if ( data[percentage] < -0.1 ) {
                className = "fa fa-arrow-down";
                classCard = "negative";
                color = DOWNRED;
            }
            else {
                className = "fa fa-minus";
                classCard = "neutral";
                color = MIDYELLOW;
            }
            // Update first line elements
            arrowImg.className = className;
            arrowImg.style.color = color;
            coloredPercentage.style.color = color;
            coloredPercentage.textContent = `${data[percentage]}%`;
            // Append them to div
            firstLineDiv.appendChild( arrowImg );
            firstLineDiv.appendChild( coloredPercentage );
            // Wrapping body divs
            bodyCard.append( firstLineDiv);
            bodyCard.append( secondLineDiv );
            // Append elements to mini-tile
            statDiv.append( cardTitle );
            statDiv.append( bodyCard );
            statDiv.classList.add( classCard );
        }
        catch ( error: any ) {
            console.log( error.message )    
        };
    };
    // Open Card
    const openDiv = document.createElement("div");
    openDiv.id = "open-div";
    openDiv.className = "div-stat";
    appendCardContent(openDiv, "open_percentage", "open");
    // Close card
    const closeDiv = document.createElement("div");
    closeDiv.id = "close-div";
    closeDiv.className = "div-stat";
    appendCardContent(closeDiv, "close_percentage", "close");
    // Low card
    const lowDiv = document.createElement("div");
    lowDiv.id = "low-div";
    lowDiv.className = "div-stat";
    appendCardContent(lowDiv, "low_percentage", "low");
    // High card
    const highDiv = document.createElement("div");
    highDiv.id = "high-div";
    highDiv.className = "div-stat";
    appendCardContent(highDiv, "high_percentage", "high");


    if ( stockTileDiv !== null ) {
        stockTileDiv.appendChild( openDiv );
        stockTileDiv.appendChild( closeDiv );
        stockTileDiv.appendChild( lowDiv );
        stockTileDiv.appendChild( highDiv );
    }
    else {
        throw Error();
    }
}

const stockPlotlyData = ( data: TrendSeries ) => {
    var mainTrace: Plotly.Data[] = [
        {
            x: data.stock_date,
            y: data.open,
            mode: 'lines',
            name:'Daily opening value',
            line: {
                color: MIDBLUE,
                width: 2,
            }
        },
        {
            x: data.stock_date,
            y: data.close,
            mode: 'lines',
            name:'Daily closing value',
            line: {
                color: DARKBLUE,
                width: 2,
            }
        },
        {
            x: data.stock_date,
            y: data.low,
            mode: 'lines',
            name:'Daily lowest value',
            line: {
                color: LAVENDER,
                width: 2,
            }
        },
        {
            x: data.stock_date,
            y: data.high,
            mode: 'lines',
            name:'Daily highest value',
            line: {
                color: LIGHTBLUE,
                width: 2,
            }
        }
    ]
    const stockLayout: Partial<Plotly.Layout> = {
        xaxis: {
            type: 'date',
        },
        yaxis: {
            title: {
                text: "Price (EUR)"
            },
        },
        font: {
            family: 'Arial',
            size: 14,
            color: 'black',
        },
    }
    if( firstTime ) {
        Plotly.newPlot('stock-chart', mainTrace, stockLayout, {responsive: true} );
    }
    else {
        Plotly.react('stock-chart', mainTrace, stockLayout);
    }
}

const kwPlotlyData = ( data: any ) => {
    let trendArray: Plotly.Data[] = []
    for ( let keyword in data ) {
        const kw_trend = {
            x: data[keyword]["kw_date"],
            y: data[keyword]["daily_search_amount"],
            mode: 'lines',
            name: `${keyword[0].toUpperCase()}${keyword.slice(1)}`,
        };
        trendArray.push(kw_trend);
    }
    
    const stockLayout: Partial<Plotly.Layout> = {
        xaxis: {
            type: 'date',
        },
        yaxis: {
            title: {
                text: "Search amount"
            },
        },
        font: {
            family: 'Arial',
            size: 14,
            color: 'black',
        },
    }
    if( firstTime ) {
        Plotly.newPlot('kw-chart', trendArray, stockLayout, {responsive: true} );
    }
    else {
        Plotly.react('kw-chart', trendArray, stockLayout );
    }
}

const displayKWStats = ( data: any ) => {
    for ( const kw in data ) {
        // Create div for each keyword
        const kwDiv = document.createElement("div");
        kwDiv.id = `stat-${kw}`;
        kwDiv.className = 'row';
        // Adding element within div
        const kwName = document.createElement("div");
        kwName.classList.add("cell");
        kwName.textContent = `${kw[0].toUpperCase()}${kw.slice(1)}`;

        const kwTodayValue = document.createElement("div");
        kwTodayValue.classList.add("cell");
        kwTodayValue.textContent = `${data[kw]["daily_search_amount"]}`;
        
        const kwOldValue = document.createElement("div");
        kwOldValue.classList.add("cell");
        kwOldValue.textContent = `${data[kw]["lagged_amount"]}`;

        // Append elements to div
        kwDiv.appendChild(kwName);
        kwDiv.appendChild(kwTodayValue);
        kwDiv.appendChild(kwOldValue);
        // Append div to main tile div
        if ( kwTileDiv !== null) {
            kwTileDiv.appendChild(kwDiv);
        };
    }

}

( async() => {
    // First landing
    // Stock part
    const stockData = await fetchTrends( undefined, getStockURL );
    stockPlotlyData( stockData );
    const stockStats = await fetchStats( getStockStatURL );
    displayStockStats( stockStats );
    // Keyword part
    const kwData = await fetchTrends( undefined, getKeywordURL );
    kwPlotlyData( kwData );
    const kwStats = await fetchStats( getKeywordStatURL );
    displayKWStats ( kwStats );
    // Update firstTime variable after first landing
    firstTime = false;
    // Event listeners
    stockDateBtns.forEach(item => {
        item.addEventListener('click', async() => {
            const startRange = getStartDate( item.id, STOCK_INTERVAL );
            const filteredData = await fetchTrends( startRange, getStockURL );
            stockPlotlyData( filteredData );
        });
    });
    kwDateBtns.forEach( item => {
        item.addEventListener('click', async() => {
            const startRange = getStartDate( item.id, KW_INTERVAL );
            const filteredData = await fetchTrends( startRange, getKeywordURL );
            kwPlotlyData( filteredData );
        })
    })
})()