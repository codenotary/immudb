package faker

import (
	"strconv"
	"strings"
)

var (
	colorLetters = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"}

	safeColorNames = []string{"black", "maroon", "green", "navy", "olive",
		"purple", "teal", "lime", "blue", "silver",
		"gray", "yellow", "fuchsia", "aqua", "white"}

	allColorNames = []string{"AliceBlue", "AntiqueWhite", "Aqua", "Aquamarine",
		"Azure", "Beige", "Bisque", "Black", "BlanchedAlmond",
		"Blue", "BlueViolet", "Brown", "BurlyWood", "CadetBlue",
		"Chartreuse", "Chocolate", "Coral", "CornflowerBlue",
		"Cornsilk", "Crimson", "Cyan", "DarkBlue", "DarkCyan",
		"DarkGoldenRod", "DarkGray", "DarkGreen", "DarkKhaki",
		"DarkMagenta", "DarkOliveGreen", "Darkorange", "DarkOrchid",
		"DarkRed", "DarkSalmon", "DarkSeaGreen", "DarkSlateBlue",
		"DarkSlateGray", "DarkTurquoise", "DarkViolet", "DeepPink",
		"DeepSkyBlue", "DimGray", "DimGrey", "DodgerBlue", "FireBrick",
		"FloralWhite", "ForestGreen", "Fuchsia", "Gainsboro", "GhostWhite",
		"Gold", "GoldenRod", "Gray", "Green", "GreenYellow", "HoneyDew",
		"HotPink", "IndianRed", "Indigo", "Ivory", "Khaki", "Lavender",
		"LavenderBlush", "LawnGreen", "LemonChiffon", "LightBlue", "LightCoral",
		"LightCyan", "LightGoldenRodYellow", "LightGray", "LightGreen", "LightPink",
		"LightSalmon", "LightSeaGreen", "LightSkyBlue", "LightSlateGray", "LightSteelBlue",
		"LightYellow", "Lime", "LimeGreen", "Linen", "Magenta", "Maroon", "MediumAquaMarine",
		"MediumBlue", "MediumOrchid", "MediumPurple", "MediumSeaGreen", "MediumSlateBlue",
		"MediumSpringGreen", "MediumTurquoise", "MediumVioletRed", "MidnightBlue",
		"MintCream", "MistyRose", "Moccasin", "NavajoWhite", "Navy", "OldLace", "Olive",
		"OliveDrab", "Orange", "OrangeRed", "Orchid", "PaleGoldenRod", "PaleGreen",
		"PaleTurquoise", "PaleVioletRed", "PapayaWhip", "PeachPuff", "Peru", "Pink", "Plum",
		"PowderBlue", "Purple", "Red", "RosyBrown", "RoyalBlue", "SaddleBrown", "Salmon",
		"SandyBrown", "SeaGreen", "SeaShell", "Sienna", "Silver", "SkyBlue", "SlateBlue",
		"SlateGray", "Snow", "SpringGreen", "SteelBlue", "Tan", "Teal", "Thistle", "Tomato",
		"Turquoise", "Violet", "Wheat", "White", "WhiteSmoke", "Yellow", "YellowGreen"}
)

type Color struct {
	Faker *Faker
}

func (c Color) Hex() string {
	color := "#"

	for i := 0; i < 6; i++ {
		color = color + c.Faker.RandomStringElement(colorLetters)
	}

	return color
}

func (c Color) RGB() string {
	color := strconv.Itoa(c.Faker.IntBetween(0, 255))

	for i := 0; i < 2; i++ {
		color = color + "," + strconv.Itoa(c.Faker.IntBetween(0, 255))
	}

	return color
}

func (c Color) RGBAsArray() [3]string {
	split := strings.Split(c.RGB(), ",")
	return [3]string{split[0], split[1], split[2]}
}

func (c Color) CSS() string {
	return "rgb(" + c.RGB() + ")"
}

func (c Color) SafeColorName() string {
	return c.Faker.RandomStringElement(safeColorNames)
}

func (c Color) ColorName() string {
	return c.Faker.RandomStringElement(allColorNames)
}
