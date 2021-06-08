package faker

import (
	"strconv"
	"strings"
)

var (
	cityPrefix = []string{"North", "East", "West", "South", "New", "Lake", "Port"}

	citySuffix = []string{"town", "ton", "land", "ville", "berg", "burgh", "borough", "bury", "view", "port", "mouth", "stad", "furt", "chester", "mouth", "fort", "haven", "side", "shire"}

	buildingNumber = []string{"%####", "%###", "%##"}

	streetSuffix = []string{"Alley", "Avenue",
		"Branch", "Bridge", "Brook", "Brooks", "Burg", "Burgs", "Bypass",
		"Camp", "Canyon", "Cape", "Causeway", "Center", "Centers", "Circle", "Circles", "Cliff", "Cliffs", "Club", "Common", "Corner", "Corners", "Course", "Court", "Courts", "Cove", "Coves", "Creek", "Crescent", "Crest", "Crossing", "Crossroad", "Curve",
		"Dale", "Dam", "Divide", "Drive", "Drive", "Drives",
		"Estate", "Estates", "Expressway", "Extension", "Extensions",
		"Fall", "Falls", "Ferry", "Field", "Fields", "Flat", "Flats", "Ford", "Fords", "Forest", "Forge", "Forges", "Fork", "Forks", "Fort", "Freeway",
		"Garden", "Gardens", "Gateway", "Glen", "Glens", "Green", "Greens", "Grove", "Groves",
		"Harbor", "Harbors", "Haven", "Heights", "Highway", "Hill", "Hills", "Hollow",
		"Inlet", "Inlet", "Island", "Island", "Islands", "Islands", "Isle", "Isle",
		"Junction", "Junctions",
		"Key", "Keys", "Knoll", "Knolls",
		"Lake", "Lakes", "Land", "Landing", "Lane", "Light", "Lights", "Loaf", "Lock", "Locks", "Locks", "Lodge", "Lodge", "Loop",
		"Mall", "Manor", "Manors", "Meadow", "Meadows", "Mews", "Mill", "Mills", "Mission", "Mission", "Motorway", "Mount", "Mountain", "Mountain", "Mountains", "Mountains",
		"Neck",
		"Orchard", "Oval", "Overpass",
		"Park", "Parks", "Parkway", "Parkways", "Pass", "Passage", "Path", "Pike", "Pine", "Pines", "Place", "Plain", "Plains", "Plains", "Plaza", "Plaza", "Point", "Points", "Port", "Port", "Ports", "Ports", "Prairie", "Prairie",
		"Radial", "Ramp", "Ranch", "Rapid", "Rapids", "Rest", "Ridge", "Ridges", "River", "Road", "Road", "Roads", "Roads", "Route", "Row", "Rue", "Run",
		"Shoal", "Shoals", "Shore", "Shores", "Skyway", "Spring", "Springs", "Springs", "Spur", "Spurs", "Square", "Square", "Squares", "Squares", "Station", "Station", "Stravenue", "Stravenue", "Stream", "Stream", "Street", "Street", "Streets", "Summit", "Summit",
		"Terrace", "Throughway", "Trace", "Track", "Trafficway", "Trail", "Trail", "Tunnel", "Tunnel", "Turnpike", "Turnpike",
		"Underpass", "Union", "Unions",
		"Valley", "Valleys", "Via", "Viaduct", "View", "Views", "Village", "Village", "Villages", "Ville", "Vista", "Vista",
		"Walk", "Walks", "Wall", "Way", "Ways", "Well", "Wells"}

	postCode = []string{"#####", "#####-####"}

	state = []string{"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"}

	stateAbbr = []string{"AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"}

	country = []string{"Afghanistan", "Albania", "Algeria", "American Samoa", "Andorra", "Angola", "Anguilla", "Antarctica (the territory South of 60 deg S)", "Antigua and Barbuda", "Argentina", "Armenia", "Aruba", "Australia", "Austria", "Azerbaijan",
		"Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bermuda", "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Bouvet Island (Bouvetoya)", "Brazil", "British Indian Ocean Territory (Chagos Archipelago)", "British Virgin Islands", "Brunei Darussalam", "Bulgaria", "Burkina Faso", "Burundi",
		"Cambodia", "Cameroon", "Canada", "Cape Verde", "Cayman Islands", "Central African Republic", "Chad", "Chile", "China", "Christmas Island", "Cocos (Keeling) Islands", "Colombia", "Comoros", "Congo", "Cook Islands", "Costa Rica", "Cote d\"Ivoire", "Croatia", "Cuba", "Cyprus", "Czech Republic",
		"Denmark", "Djibouti", "Dominica", "Dominican Republic",
		"Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Ethiopia",
		"Faroe Islands", "Falkland Islands (Malvinas)", "Fiji", "Finland", "France", "French Guiana", "French Polynesia", "French Southern Territories",
		"Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Gibraltar", "Greece", "Greenland", "Grenada", "Guadeloupe", "Guam", "Guatemala", "Guernsey", "Guinea", "Guinea-Bissau", "Guyana",
		"Haiti", "Heard Island and McDonald Islands", "Holy See (Vatican City State)", "Honduras", "Hong Kong", "Hungary",
		"Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Isle of Man", "Israel", "Italy",
		"Jamaica", "Japan", "Jersey", "Jordan",
		"Kazakhstan", "Kenya", "Kiribati", "Korea", "Korea", "Kuwait", "Kyrgyz Republic",
		"Lao People\"s Democratic Republic", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libyan Arab Jamahiriya", "Liechtenstein", "Lithuania", "Luxembourg",
		"Macao", "Macedonia", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Martinique", "Mauritania", "Mauritius", "Mayotte", "Mexico", "Micronesia", "Moldova", "Monaco", "Mongolia", "Montenegro", "Montserrat", "Morocco", "Mozambique", "Myanmar",
		"Namibia", "Nauru", "Nepal", "Netherlands Antilles", "Netherlands", "New Caledonia", "New Zealand", "Nicaragua", "Niger", "Nigeria", "Niue", "Norfolk Island", "Northern Mariana Islands", "Norway",
		"Oman",
		"Pakistan", "Palau", "Palestinian Territories", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Pitcairn Islands", "Poland", "Portugal", "Puerto Rico",
		"Qatar",
		"Reunion", "Romania", "Russian Federation", "Rwanda",
		"Saint Barthelemy", "Saint Helena", "Saint Kitts and Nevis", "Saint Lucia", "Saint Martin", "Saint Pierre and Miquelon", "Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia (Slovak Republic)", "Slovenia", "Solomon Islands", "Somalia", "South Africa", "South Georgia and the South Sandwich Islands", "Spain", "Sri Lanka", "Sudan", "Suriname", "Svalbard & Jan Mayen Islands", "Swaziland", "Sweden", "Switzerland", "Syrian Arab Republic",
		"Taiwan", "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo", "Tokelau", "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey", "Turkmenistan", "Turks and Caicos Islands", "Tuvalu",
		"Uganda", "Ukraine", "United Arab Emirates", "United Kingdom", "United States of America", "United States Minor Outlying Islands", "United States Virgin Islands", "Uruguay", "Uzbekistan",
		"Vanuatu", "Venezuela", "Vietnam",
		"Wallis and Futuna", "Western Sahara",
		"Yemen",
		"Zambia", "Zimbabwe"}

	cityFormats = []string{"{{cityPrefix}} {{firstName}}{{citySuffix}}",
		"{{cityPrefix}} {{firstName}}",
		"{{firstName}}{{citySuffix}}",
		"{{lastName}}{{citySuffix}}"}

	streetNameFormats = []string{"{{firstName}} {{streetSuffix}}",
		"{{lastName}} {{streetSuffix}}"}

	streetAddressFormats = []string{"{{buildingNumber}} {{streetName}}",
		"{{buildingNumber}} {{streetName}} {{secondaryAddress}}"}

	addressFormats = []string{"{{streetAddress}}\n{{city}}, {{stateAbbr}} {{postCode}}"}

	secondaryAddressFormats = []string{"Apt. ###", "Suite ###"}
)

type Address struct {
	Faker *Faker
}

func (a Address) CityPrefix() string {
	return a.Faker.RandomStringElement(cityPrefix)
}

func (a Address) SecondaryAddress() string {
	format := a.Faker.RandomStringElement(secondaryAddressFormats)
	return a.Faker.Bothify(format)
}

func (a Address) State() string {
	return a.Faker.RandomStringElement(state)
}

func (a Address) StateAbbr() string {
	return a.Faker.RandomStringElement(stateAbbr)
}

func (a Address) CitySuffix() string {
	return a.Faker.RandomStringElement(citySuffix)
}

func (a Address) StreetSuffix() string {
	return a.Faker.RandomStringElement(streetSuffix)
}

func (a Address) BuildingNumber() (bn string) {
	t := a.Faker.IntBetween(1, 6)
	for i := 0; i < t; i++ {
		bn = bn + strconv.Itoa(a.Faker.RandomDigitNotNull())
	}

	return
}

func (a Address) City() string {
	city := a.Faker.RandomStringElement(cityFormats)

	// {{cityPrefix}}
	if strings.Contains(city, "{{cityPrefix}}") {
		city = strings.Replace(city, "{{cityPrefix}}", a.CityPrefix(), 1)
	}

	var p Person = a.Faker.Person()

	// {{firstName}}
	if strings.Contains(city, "{{firstName}}") {
		city = strings.Replace(city, "{{firstName}}", p.FirstName(), 1)
	}

	// {{lastName}}
	if strings.Contains(city, "{{lastName}}") {
		city = strings.Replace(city, "{{lastName}}", p.LastName(), 1)
	}

	// {{citySuffix}}
	if strings.Contains(city, "{{citySuffix}}") {
		city = strings.Replace(city, "{{citySuffix}}", a.CitySuffix(), 1)
	}

	return city
}

func (a Address) StreetName() string {
	street := a.Faker.RandomStringElement(streetNameFormats)

	var p Person = a.Faker.Person()

	// {{firstName}}
	if strings.Contains(street, "{{firstName}}") {
		street = strings.Replace(street, "{{firstName}}", p.FirstName(), 1)
	}

	// {{lastName}}
	if strings.Contains(street, "{{lastName}}") {
		street = strings.Replace(street, "{{lastName}}", p.LastName(), 1)
	}

	// {{streetSuffix}}
	if strings.Contains(street, "{{streetSuffix}}") {
		street = strings.Replace(street, "{{streetSuffix}}", a.StreetSuffix(), 1)
	}

	return street
}

func (a Address) StreetAddress() string {
	streetAddress := a.Faker.RandomStringElement(streetAddressFormats)

	// {{buildingNumber}}
	if strings.Contains(streetAddress, "{{buildingNumber}}") {
		streetAddress = strings.Replace(streetAddress, "{{buildingNumber}}", a.BuildingNumber(), 1)
	}

	// {{streetName}}
	if strings.Contains(streetAddress, "{{streetName}}") {
		streetAddress = strings.Replace(streetAddress, "{{streetName}}", a.StreetName(), 1)
	}

	// {{secondaryAddress}}
	if strings.Contains(streetAddress, "{{secondaryAddress}}") {
		streetAddress = strings.Replace(streetAddress, "{{secondaryAddress}}", a.SecondaryAddress(), 1)
	}

	return streetAddress
}

func (a Address) PostCode() string {
	format := a.Faker.RandomStringElement(postCode)
	return a.Faker.Bothify(format)
}

func (a Address) Address() string {
	address := a.Faker.RandomStringElement(addressFormats)

	// {{streetAddress}}
	if strings.Contains(address, "{{streetAddress}}") {
		address = strings.Replace(address, "{{streetAddress}}", a.StreetAddress(), 1)
	}

	// {{city}}
	if strings.Contains(address, "{{city}}") {
		address = strings.Replace(address, "{{city}}", a.City(), 1)
	}

	// {{stateAbbr}}
	if strings.Contains(address, "{{stateAbbr}}") {
		address = strings.Replace(address, "{{stateAbbr}}", a.StateAbbr(), 1)
	}

	// {{postCode}}
	if strings.Contains(address, "{{postCode}}") {
		address = strings.Replace(address, "{{postCode}}", a.PostCode(), 1)
	}

	return address
}

func (a Address) Country() string {
	return a.Faker.RandomStringElement(country)
}

func (a Address) Latitude() (latitude float64) {
	latitude, _ = strconv.ParseFloat(a.Faker.Numerify("##.######"), 64)
	return
}

func (a Address) Longitude() (latitude float64) {
	latitude, _ = strconv.ParseFloat(a.Faker.Numerify("##.######"), 64)
	return
}
