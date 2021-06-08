package faker

import (
	"strconv"
)

var (
	cardVendors = []string{
		"Visa", "Visa", "Visa", "Visa", "Visa",
		"MasterCard", "MasterCard", "MasterCard", "MasterCard", "MasterCard",
		"American Express", "Discover Card", "Visa Retired"}
)

type Payment struct {
	Faker *Faker
}

func (p Payment) CreditCardType() string {
	return p.Faker.RandomStringElement(cardVendors)
}

func (p Payment) CreditCardNumber() string {
	return strconv.Itoa(p.Faker.IntBetween(1000000000000000, 9999999999999999))
}

func (p Payment) CreditCardExpirationDateString() string {
	day := strconv.Itoa(p.Faker.IntBetween(0, 30))
	if len(day) == 1 {
		day = "0" + day
	}

	month := strconv.Itoa(p.Faker.IntBetween(12, 30))

	return day + "/" + month
}
