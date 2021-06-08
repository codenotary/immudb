package faker

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Faker struct {
	Generator *rand.Rand
}

func (f Faker) RandomDigit() int {
	return f.Generator.Int() % 10
}

func (f Faker) RandomDigitNot(ignore ...int) int {
	inSlice := func(el int, list []int) bool {
		for i := range list {
			if i == el {
				return true
			}
		}

		return false
	}

	for {
		current := f.RandomDigit()
		if inSlice(current, ignore) {
			return current
		}
	}
}

func (f Faker) RandomDigitNotNull() int {
	return f.Generator.Int()%8 + 1
}

func (f Faker) RandomNumber(size int) int {
	if size == 1 {
		return f.RandomDigit()
	}

	var min int = int(math.Pow10(size - 1))
	var max int = int(math.Pow10(size)) - 1

	return f.IntBetween(min, max)
}

func (f Faker) RandomFloat(maxDecimals, min, max int) float64 {
	s := fmt.Sprintf("%d.%d", f.IntBetween(min, max-1), f.IntBetween(1, maxDecimals))
	value, _ := strconv.ParseFloat(s, 10)
	return value
}

func (f Faker) IntBetween(min, max int) int {
	diff := max - min

	if diff == 0 {
		return min
	}

	return min + (f.Generator.Int() % diff)
}

func (f Faker) Int64Between(min, max int64) int64 {
	return int64(f.IntBetween(int(min), int(max)))
}

func (f Faker) Int32Between(min, max int32) int32 {
	return int32(f.IntBetween(int(min), int(max)))
}

func (f Faker) RandomLetter() string {
	return string(f.IntBetween(97, 122))
}

func (f Faker) RandomStringElement(s []string) string {
	i := f.IntBetween(0, len(s)-1)
	return s[i]
}

func (f Faker) RandomIntElement(a []int) int {
	i := f.IntBetween(0, len(a)-1)
	return a[i]
}

func (f Faker) ShuffleString(s string) string {
	orig := strings.Split(s, "")
	dest := make([]string, len(orig))

	for i := 0; i < len(orig); i++ {
		dest[i] = orig[len(orig)-i-1]
	}

	return strings.Join(dest, "")
}

func (f Faker) Numerify(in string) (out string) {
	for _, c := range strings.Split(in, "") {
		if c == "#" {
			c = strconv.Itoa(f.RandomDigit())
		}

		out = out + c
	}

	return
}

func (f Faker) Lexify(in string) (out string) {
	for _, c := range strings.Split(in, "") {
		if c == "?" {
			c = f.RandomLetter()
		}

		out = out + c
	}

	return
}

func (f Faker) Bothify(in string) (out string) {
	out = f.Lexify(in)
	out = f.Numerify(out)
	return
}

func (f Faker) Asciify(in string) (out string) {
	for _, c := range strings.Split(in, "") {
		if c == "*" {
			c = string(f.IntBetween(97, 126))
		}

		out = out + c
	}

	return
}

func (f Faker) Bool() bool {
	return f.IntBetween(0, 100) > 50
}

func (f Faker) Lorem() Lorem {
	return Lorem{&f}
}

func (f Faker) Person() Person {
	return Person{&f}
}

func (f Faker) Address() Address {
	return Address{&f}
}

func (f Faker) Phone() Phone {
	return Phone{&f}
}

func (f Faker) Company() Company {
	return Company{&f}
}

func (f Faker) Time() Time {
	return Time{&f}
}

func (f Faker) Internet() Internet {
	return Internet{&f}
}

func (f Faker) UserAgent() UserAgent {
	return UserAgent{&f}
}

func (f Faker) Payment() Payment {
	return Payment{&f}
}

func (f Faker) Color() Color {
	return Color{&f}
}

func (f Faker) UUID() UUID {
	return UUID{&f}
}

func New() (f Faker) {
	seed := rand.NewSource(time.Now().Unix())
	f = NewWithSeed(seed)
	return
}

func NewWithSeed(src rand.Source) (f Faker) {
	generator := rand.New(src)
	f = Faker{Generator: generator}
	return
}
