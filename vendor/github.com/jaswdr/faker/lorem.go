package faker

import (
	"strings"
)

var (
	wordsList = []string{"alias", "consequatur", "aut", "perferendis", "sit", "voluptatem", "accusantium", "doloremque", "aperiam", "eaque", "ipsa", "quae", "ab", "illo", "inventore", "veritatis", "et", "quasi", "architecto", "beatae", "vitae", "dicta", "sunt", "explicabo", "aspernatur", "aut", "odit", "aut", "fugit", "sed", "quia", "consequuntur", "magni", "dolores", "eos", "qui", "ratione", "voluptatem", "sequi", "nesciunt", "neque", "dolorem", "ipsum", "quia", "dolor", "sit", "amet", "consectetur", "adipisci", "velit", "sed", "quia", "non", "numquam", "eius", "modi", "tempora", "incidunt", "ut", "labore", "et", "dolore", "magnam", "aliquam", "quaerat", "voluptatem", "ut", "enim", "ad", "minima", "veniam", "quis", "nostrum", "exercitationem", "ullam", "corporis", "nemo", "enim", "ipsam", "voluptatem", "quia", "voluptas", "sit", "suscipit", "laboriosam", "nisi", "ut", "aliquid", "ex", "ea", "commodi", "consequatur", "quis", "autem", "vel", "eum", "iure", "reprehenderit", "qui", "in", "ea", "voluptate", "velit", "esse", "quam", "nihil", "molestiae", "et", "iusto", "odio", "dignissimos", "ducimus", "qui", "blanditiis", "praesentium", "laudantium", "totam", "rem", "voluptatum", "deleniti", "atque", "corrupti", "quos", "dolores", "et", "quas", "molestias", "excepturi", "sint", "occaecati", "cupiditate", "non", "provident", "sed", "ut", "perspiciatis", "unde", "omnis", "iste", "natus", "error", "similique", "sunt", "in", "culpa", "qui", "officia", "deserunt", "mollitia", "animi", "id", "est", "laborum", "et", "dolorum", "fuga", "et", "harum", "quidem", "rerum", "facilis", "est", "et", "expedita", "distinctio", "nam", "libero", "tempore", "cum", "soluta", "nobis", "est", "eligendi", "optio", "cumque", "nihil", "impedit", "quo", "porro", "quisquam", "est", "qui", "minus", "id", "quod", "maxime", "placeat", "facere", "possimus", "omnis", "voluptas", "assumenda", "est", "omnis", "dolor", "repellendus", "temporibus", "autem", "quibusdam", "et", "aut", "consequatur", "vel", "illum", "qui", "dolorem", "eum", "fugiat", "quo", "voluptas", "nulla", "pariatur", "at", "vero", "eos", "et", "accusamus", "officiis", "debitis", "aut", "rerum", "necessitatibus", "saepe", "eveniet", "ut", "et", "voluptates", "repudiandae", "sint", "et", "molestiae", "non", "recusandae", "itaque", "earum", "rerum", "hic", "tenetur", "a", "sapiente", "delectus", "ut", "aut", "reiciendis", "voluptatibus", "maiores", "doloribus", "asperiores", "repellat"}
)

type Lorem struct {
	Faker *Faker
}

func (l Lorem) Word() string {
	index := l.Faker.IntBetween(0, len(wordsList)-1)
	return wordsList[index]
}

func (l Lorem) Words(nbWords int) (words []string) {
	for i := 0; i < nbWords; i++ {
		words = append(words, l.Word())
	}

	return
}

func (l Lorem) Sentence(nbWords int) string {
	return strings.Join(l.Words(nbWords), " ") + "."
}

func (l Lorem) Sentences(nbSentences int) (sentences []string) {
	for i := 0; i < nbSentences; i++ {
		sentences = append(sentences, l.Sentence(l.Faker.RandomNumber(2)))
	}

	return
}

func (l Lorem) Paragraph(nbSentences int) string {
	return strings.Join(l.Sentences(nbSentences), " ")
}

func (l Lorem) Paragraphs(nbParagraph int) (out []string) {
	for i := 0; i < nbParagraph; i++ {
		out = append(out, l.Paragraph(l.Faker.RandomNumber(2)))
	}

	return
}

func (l Lorem) Text(maxNbChars int) (out string) {
	for _, w := range wordsList {
		if len(out)+len(w) > maxNbChars {
			break
		}

		out = out + w
	}

	return
}

func (l Lorem) Bytes(maxNbChars int) (out []byte) {
	return []byte(l.Text(maxNbChars))
}
