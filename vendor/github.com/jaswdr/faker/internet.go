package faker

import (
	"net/http"
	"strconv"
	"strings"
)

var (
	freeEmailDomain = []string{"gmail.com", "yahoo.com", "hotmail.com"}

	tld = []string{"com", "com", "com", "com", "com", "com", "biz", "info", "net", "org"}

	userFormats = []string{"{{lastName}}.{{firstName}}",
		"{{firstName}}.{{lastName}}",
		"{{firstName}}",
		"{{lastName}}"}

	emailFormats = []string{"{{user}}@{{domain}}", "{{user}}@{{freeEmailDomain}}", ""}

	urlFormats = []string{"http://www.{{domain}}/",
		"http://{{domain}}/",
		"http://www.{{domain}}/{{slug}}",
		"http://www.{{domain}}/{{slug}}",
		"https://www.{{domain}}/{{slug}}",
		"http://www.{{domain}}/{{slug}}.html",
		"http://{{domain}}/{{slug}}",
		"http://{{domain}}/{{slug}}",
		"http://{{domain}}/{{slug}}.html",
		"https://{{domain}}/{{slug}}.html",
	}
)

type Internet struct {
	Faker *Faker
}

func (i Internet) User() string {
	user := i.Faker.RandomStringElement(userFormats)

	p := i.Faker.Person()

	// {{firstName}}
	if strings.Contains(user, "{{firstName}}") {
		user = strings.Replace(user, "{{firstName}}", strings.ToLower(p.FirstName()), 1)
	}

	// {{lastName}}
	if strings.Contains(user, "{{lastName}}") {
		user = strings.Replace(user, "{{lastName}}", strings.ToLower(p.LastName()), 1)
	}

	return user
}

func (i Internet) Password() string {
	pattern := strings.Repeat("*", i.Faker.IntBetween(6, 16))
	return i.Faker.Asciify(pattern)
}

func (i Internet) Domain() string {
	domain := strings.ToLower(i.Faker.Lexify("???"))
	return strings.Join([]string{domain, i.TLD()}, ".")
}

func (i Internet) FreeEmailDomain() string {
	return i.Faker.RandomStringElement(freeEmailDomain)
}

func (i Internet) SafeEmailDomain() string {
	return "example.org"
}

func (i Internet) Email() string {
	email := i.Faker.RandomStringElement(emailFormats)

	// {{user}}
	if strings.Contains(email, "{{user}}") {
		email = strings.Replace(email, "{{user}}", i.User(), 1)
	}

	// {{domain}}
	if strings.Contains(email, "{{domain}}") {
		email = strings.Replace(email, "{{domain}}", i.Domain(), 1)
	}

	// {{freeEmailDomain}}
	if strings.Contains(email, "{{freeEmailDomain}}") {
		email = strings.Replace(email, "{{freeEmailDomain}}", i.FreeEmailDomain(), 1)
	}

	return email
}

func (i Internet) FreeEmail() string {
	domain := i.Faker.RandomStringElement(freeEmailDomain)

	return strings.Join([]string{i.User(), domain}, "@")
}

func (i Internet) SafeEmail() string {
	return strings.Join([]string{i.User(), i.SafeEmailDomain()}, "@")
}

func (i Internet) CompanyEmail() string {
	c := i.Faker.Company()

	companyName := c.Name()
	companyName = strings.ToLower(companyName)
	companyName = strings.Replace(companyName, " ", ".", 0)

	domain := strings.Join([]string{companyName, i.Domain()}, ".")

	return strings.Join([]string{i.User(), domain}, "@")
}

func (i Internet) TLD() string {
	return i.Faker.RandomStringElement(tld)
}

func (i Internet) Slug() string {
	slug := strings.Repeat("?", i.Faker.IntBetween(1, 5))
	slug = slug + "-"
	slug = strings.Repeat("?", i.Faker.IntBetween(1, 6))
	slug = i.Faker.Lexify(slug)

	return strings.ToLower(slug)
}

func (i Internet) URL() string {
	url := i.Faker.RandomStringElement(urlFormats)

	// {{domain}}
	if strings.Contains(url, "{{domain}}") {
		url = strings.Replace(url, "{{domain}}", i.Domain(), 1)
	}

	// {{slug}}
	if strings.Contains(url, "{{slug}}") {
		url = strings.Replace(url, "{{slug}}", i.Slug(), 1)
	}

	return url
}

func (i Internet) Ipv4() string {
	ips := []string{}

	for j := 0; j < 4; j++ {
		ips = append(ips, strconv.Itoa(i.Faker.IntBetween(1, 255)))
	}

	return strings.Join(ips, ".")
}

func (i Internet) LocalIpv4() string {
	ips := []string{i.Faker.RandomStringElement([]string{"10", "172", "192"})}

	if ips[0] == "10" {
		for j := 0; j < 3; j++ {
			ips = append(ips, strconv.Itoa(i.Faker.IntBetween(1, 255)))
		}
	}

	if ips[0] == "172" {
		ips = append(ips, strconv.Itoa(i.Faker.IntBetween(16, 31)))

		for j := 0; j < 2; j++ {
			ips = append(ips, strconv.Itoa(i.Faker.IntBetween(1, 255)))
		}
	}

	if ips[0] == "192" {
		ips = append(ips, "168")

		for j := 0; j < 2; j++ {
			ips = append(ips, strconv.Itoa(i.Faker.IntBetween(1, 255)))
		}
	}

	return strings.Join(ips, ".")
}

func (i Internet) Ipv6() string {
	ips := []string{}

	for j := 0; j < 8; j++ {
		block := ""
		for w := 0; w < 4; w++ {
			block = block + strconv.Itoa(i.Faker.RandomDigitNotNull())
		}

		ips = append(ips, block)
	}

	return strings.Join(ips, ":")
}

func (i Internet) MacAddress() string {
	values := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"}

	mac := []string{}
	for j := 0; j < 6; j++ {
		m := i.Faker.RandomStringElement(values)
		m = m + i.Faker.RandomStringElement(values)
		mac = append(mac, m)
	}

	return strings.Join(mac, ":")
}

func (i Internet) HTTPMethod() string {
	return i.Faker.RandomStringElement([]string{
		http.MethodGet,
		http.MethodHead,
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodConnect,
		http.MethodOptions,
		http.MethodTrace,
	})
}

func (i Internet) Query() string {
	lorem := i.Faker.Lorem()
	query := "?" + lorem.Word() + "=" + lorem.Word()
	for j := 0; j < i.Faker.IntBetween(1, 3); j++ {
		if i.Faker.Bool() {
			query += "&" + lorem.Word() + "=" + lorem.Word()
		} else {
			query += "&" + lorem.Word() + "=" + strconv.Itoa(i.Faker.RandomDigitNotNull())
		}
	}

	return query
}
