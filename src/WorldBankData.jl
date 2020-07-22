"""
Provides two functions, [`search_wdi`](@ref) and [`wdi`](@ref), for searching and fetching World Development Indicators data from the World Bank.
"""
module WorldBankData

using HTTP
using JSON
using DataFrames
using Dates

export wdi, search_wdi


function download_parse_json(url::String; verbose::Bool=false)
    if verbose
        println("download: ", url)
    end
    request = HTTP.get(url)
    if request.status != 200
        error("download failed")
    end
    JSON.parse(String(request.body))
end

# convert json from worldbank for an indicator to dataframe
function parse_indicator(json::Array{Any,1})::DataFrame
    indicator_val = String[]
    name_val = String[]
    description_val = String[]
    source_database_val = String[]
    source_organization_val = String[]

    for d in json[2]
        append!(indicator_val, [d["id"]])
        append!(name_val, [d["name"]])
        append!(description_val, [d["sourceNote"]])
        append!(source_database_val, [d["source"]["value"]])
        append!(source_organization_val, [d["sourceOrganization"]])
    end

    DataFrame(indicator=indicator_val, name=name_val,
              description=description_val, source_database=source_database_val,
              source_organization=source_organization_val)
end

function tofloat(f::AbstractString)::Union{Missing,Float64}
    x = tryparse(Float64, f)
    x isa Nothing ? missing : x
end

# convert country json to DataFrame
function parse_country(json::Array{Any,1})::DataFrame
    iso3c_val = String[]
    iso2c_val = String[]
    name_val = String[]
    region_val = String[]
    capital_val = String[]
    longitude_val = String[]
    latitude_val = String[]
    income_val = String[]
    lending_val = String[]

    for d in json[2]
        append!(iso3c_val, [d["id"]])
        append!(iso2c_val, [d["iso2Code"]])
        append!(name_val, [d["name"]])
        append!(region_val, [d["region"]["value"]])
        append!(capital_val, [d["capitalCity"]])
        append!(longitude_val, [d["longitude"]])
        append!(latitude_val, [d["latitude"]])
        append!(income_val, [d["incomeLevel"]["value"]])
        append!(lending_val, [d["lendingType"]["value"]])
    end

    longitude_val = tofloat.(longitude_val)
    latitude_val = tofloat.(latitude_val)

    DataFrame(iso3c=iso3c_val, iso2c=iso2c_val, name=name_val,
              region=region_val, capital=capital_val, longitude=longitude_val,
              latitude=latitude_val, income=income_val, lending=lending_val)
end

function download_indicators(;verbose::Bool=false)::DataFrame
    dat = download_parse_json("https://api.worldbank.org/v2/indicators?per_page=25000&format=json", verbose=verbose)

    parse_indicator(dat)
end

function download_countries(;verbose::Bool=false)::DataFrame
    dat = download_parse_json("https://api.worldbank.org/v2/countries/all?per_page=25000&format=json", verbose=verbose)

    parse_country(dat)
end

country_cache = false
indicator_cache = false

function reset_country_cache()
    global country_cache = false
end

function reset_indicator_cache()
    global indicator_cache = false
end

function set_country_cache(df::AbstractDataFrame)
    global country_cache = df
end

function set_indicator_cache(df::AbstractDataFrame)
    global indicator_cache = df
end

function get_countries(;verbose::Bool=false)
    if country_cache == false
        set_country_cache(download_countries(verbose=verbose))
    end
    country_cache
end

function get_indicators(;verbose::Bool=false)
    if indicator_cache == false
        set_indicator_cache(download_indicators(verbose=verbose))
    end
    indicator_cache
end

# The "." character is illegal in symbol, but used a lot in WDI. replace by "_".
# example: NY.GNP.PCAP.CD becomes NY_GNP_PCAP_CD
function make_symbol(x::String)::Symbol
    Symbol(replace(x, "." => "_"))
end

make_symbol(x::Symbol) = x

df_match(df::AbstractDataFrame, entry::String, regex::Regex)::DataFrame = df[occursin.(Ref(regex), df[!, make_symbol(entry)]),:]

function country_match(entry::String, regex::Regex)::DataFrame
    df = get_countries()
    df_match(df, entry, regex)
end

function indicator_match(entry::String, regex::Regex)::DataFrame
    df = get_indicators()
    df_match(df, entry, regex)
end

function search_countries(entry::String, regx::Regex)::DataFrame
    entries = ["name","region","capital","iso2c","iso3c","income","lending"]
    if !(entry in entries)
        error("unsupported country entry: \"", entry, "\". supported are:\n", entries)
    end
    country_match(entry, regx)
end

function search_indicators(entry::String, regx::Regex)::DataFrame
    entries = ["name","description","topics","source_database","source_organization"]
    if !(entry in entries)
        error("unsupported indicator entry: \"", entry, "\". supported are\n", entries)
    end
    indicator_match(entry, regx)
end


"""
search_wdi(data::String, entry::String, regx::Regex)::DataFrame

Search World Development Indicators for countries or indicators.

https://datacatalog.worldbank.org/dataset/world-development-indicators

**Arguments**

* `data` : data to search for: "indicators" or "countries"
* `entry` : entry to lookup
  for countries: `name`,`region`,`capital`,`iso2c`,`iso3c`,`income`,`lending`
  for indicators: `name`, `description`, `topics`, `source_database`, `source_organization`
* `regex` : regular expression to find

# Examples
```julia
search_wdi("countries", "name", r"united"i)
search_wdi("indicators", "description", r"gross national"i)
```
"""
function search_wdi(data::String, entry::String, regx::Regex)::DataFrame
    if data == "countries"
        return search_countries(entry, regx)
    elseif data == "indicators"
        return search_indicators(entry, regx)
    else
        error("unsupported data source:", data, ". supported are: \"countries\" or \"indicators\"")
    end
end

function clean_entry(x::Union{AbstractString,Nothing})
    if typeof(x) == Nothing
        return "NA"
    else
        return x
    end
end

function clean_append!(vals::Union{Array{String,1},Array{String,1}}, val::Union{String,String,Nothing})
    append!(vals, [clean_entry(val)])
end

function quartermonth(x::AbstractString) 
    if x == "1"
        m = "01"
    elseif x == "2"
        m = "04"
    elseif x == "3"
        m = "07"
    elseif x == "4"
        m = "10"
    else 
        m = "NA"
    end
    return m
end


function parse_date(s::AbstractString)
    if occursin("Q", s)
        return parse_quarterly(s), "Q"
    elseif occursin("M", s)
        return parse_monthly(s), "M"
    elseif length(s) == 4
        parse_yearly(s), "Y"
    else 
        missing, "NA"
    end 
end

parse_yearly(s) = Dates.Date(s, "yyyy")

parse_monthly(s) = Dates.Date(s, dateformat"yyyy\Mmm")

function parse_quarterly(s) 
    q = split(s, "Q")
    Dates.Date(q[1] * quartermonth(q[2]), dateformat"yyyymm")
end 

function parse_wdi(indicator::String, jsondata::Union{Array{Any,1}, Nothing}, startdate::Date, enddate::Date)::DataFrame
    country_id = String[]
    country_name = String[]
    value = Union{Float64,Missing}[]
    date = Date[]
    freq = String[]

    if !(jsondata isa Nothing)
        for d in jsondata
            clean_append!(country_id, d["country"]["id"])
            clean_append!(country_name, d["country"]["value"])
            push!(value, d["value"] isa Nothing ? missing : d["value"])
            date_val, freq_val = parse_date(d["date"])
            push!(date, date_val)
            push!(freq, freq_val)
        end
    end
    
    df = DataFrame(iso2c=country_id, country=country_name, frequency=freq, date=date)
    df[!, :year] = convert.(Float64, Dates.year.(date)) # for compatibility
    df[!, make_symbol(indicator)] = value
    
    filter(row -> startdate<= row[:date] <= enddate, df)
end

function get_url(indicator::String, countries::Union{String,Array{String,1}}; verbose::Bool=false)::String
    countriesstr = ""
    if typeof(countries) == String
        countriesstr = countries
    elseif typeof(countries) == Array{String,1}
        for (i, c) in enumerate(countries)
            if i == 1
                countriesstr = c
            else
                countriesstr = string(countriesstr, ";", c)
            end
        end
    end
    url = string("https://api.worldbank.org/v2/countries/", countriesstr, "/indicators/", indicator,
                 "?format=json&per_page=25000")

    url
end

function wdi_download(indicator::String, countries::Union{String,Array{String,1}}, startyear::Date, endyear::Date; verbose::Bool=false)::DataFrame
    url = get_url(indicator, countries, verbose=verbose)
    jsondata = download_parse_json(url, verbose=verbose)

    if length(jsondata) == 1
        d = jsondata[1]
        if haskey(d, "message")
            msg = d["message"][1]
            error("request error. response key=", msg["key"], " value=", msg["value"])
        end
        error("json data problem:", jsondata)
    end

    if length(jsondata) != 2
        error("wrong length json reply:", jsondata)
    end

    parse_wdi(indicator, jsondata[2], startyear, endyear)
end


"""
function wdi(indicators::Union{String,Array{String,1}}, countries::Union{String,Array{String,1}}, startyear::Integer = -1, endyear::Integer = -1; extra::Bool = false, verbose::Bool = false)::DataFrame

Download data from World Development Indicators (WDI) Data Catalog of the World Bank.

https://datacatalog.worldbank.org/dataset/world-development-indicators

**Arguments**
`indicators` : indicator name or array of indicators
`countries` : string or string array of ISO 2 or ISO 3 letter country codes or `all` for all countries.
`startyear` : first year to include
`endyear` : last year to include (required if startyear is set)
`extra` : if `true` additional country data should be included (region, capital, longitude, latitude, income, lending)
`verbose` : if `true` print URLs downloaded

# Examples
```julia
df = wdi("SP.POP.TOTL", "US", 1980, 2012, extra=true)
df = wdi("SP.POP.TOTL", "USA", 1980, 2012, extra=true)
df = wdi("SP.POP.TOTL", "all", 2000, 2000, verbose=true, extra=true) # gets all countries and regions 
df = wdi("SP.POP.TOTL", "all_countries", 2000, 2000, verbose=true, extra=true) # selects only countries, not regional aggregates
df = wdi("SP.POP.TOTL", ["US","BR"], 1980, 2012, extra=true)
df = wdi(["SP.POP.TOTL", "NY.GDP.MKTP.CD"], ["US","BR"], 1980, 2012, extra=true)
```
"""
function wdi(indicators::Union{String,Array{String,1}}, countries::Union{String,Array{String,1}}, startyear::Int, endyear::Int; extra::Bool=false, verbose::Bool=false)::DataFrame
    wdi(indicators, countries, Date(startyear), Date(endyear, 12, 31); extra=extra, verbose=verbose)
end 


function wdi(indicators::Union{String,Array{String,1}}, countries::Union{String,Array{String,1}}, startdate::Date=Date(1000), enddate::Date=Date(3000); extra::Bool=false, verbose::Bool=false)::DataFrame
    
    if typeof(countries) == String
        if countries == "all_countries"
            countries = filter(x -> (x["region"] != "Aggregates"), WorldBankData.get_countries())[:, :iso2c]  # filter out aggregates
        elseif countries == "all"
            countries = get_countries()[:, :iso2c]
        else
            countries = [countries]
        end
    end

    countries = unique(countries) # eliminate duplicated countries

    if ! (startdate <= enddate)
        error("startdate has to be < enddate. startdate=", startdate, ". enddate=", enddate)
    end

    if typeof(indicators) == String
        indicators = [indicators]
    end

    indicators = unique(indicators) # eliminate duplicated series 

    df = wdi_download(indicators[1], countries, startdate, enddate, verbose=verbose)

    if length(indicators) > 1
        noindcols = [x for x in filter(x -> !(make_symbol(x) in map(make_symbol, indicators)), names(df))]
        for ind in indicators[2:length(indicators)]
            dfn = wdi_download(ind, countries, startdate, enddate, verbose=verbose)
            df = outerjoin(df, dfn, on=noindcols)
        end
    end

    if extra
        cntdat = get_countries(verbose=verbose)
        df = innerjoin(df, cntdat, on=:iso2c)
    end

    sort!(df, [order(:iso2c), order(:year)])

    df
end

end
