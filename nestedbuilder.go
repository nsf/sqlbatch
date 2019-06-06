package sqlbatch

import "fmt"

type nestedBuilder struct {
	id   int
	name string
	b    *Batch
}

func (nb *nestedBuilder) assertNotCommitted() {
	if nb.id == 0 {
		panic(fmt.Sprintf("nested builder %s #%d already committed", nb.name, nb.id))
	}
}

func (nb *nestedBuilder) release() {
	nb.b.releaseNestedBuilder(nb)
}

func (b *Batch) releaseNestedBuilder(nb *nestedBuilder) {
	delete(b.liveNestedBuilders, nb.id)
	nb.id = 0
}

func (b *Batch) allocateNestedBuilder(name string) nestedBuilder {
	b.nextNestedBuilderID++
	id := b.nextNestedBuilderID
	if b.liveNestedBuilders == nil {
		b.liveNestedBuilders = map[int]string{}
	}
	b.liveNestedBuilders[id] = name
	return nestedBuilder{
		b:    b,
		name: name,
		id:   id,
	}
}
