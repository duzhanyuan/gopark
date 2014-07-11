package gopark

type Dependency interface {
	getRDD() RDD
	getIsShuffle() bool
}

type _BaseDependency struct {
	rdd       RDD
	isShuffle bool
}

func (d *_BaseDependency) getRDD() RDD {
	return d.rdd
}

func (d *_BaseDependency) getIsShuffle() bool {
	return d.isShuffle
}

func newBaseDependency(rdd RDD) *_BaseDependency {
	return &_BaseDependency{
		rdd: rdd,
	}
}

type _ShuffleDependency struct {
	*_BaseDependency
	shuffleId   uint64
	aggregator  *Aggregator
	partitioner Partitioner
}

func newShuffleDependency(shuffleId uint64, rdd RDD, aggregator *Aggregator,
	partitioner Partitioner) *_ShuffleDependency {
	d := &_ShuffleDependency{
		_BaseDependency: newBaseDependency(rdd),
		shuffleId:       shuffleId,
		aggregator:      aggregator,
		partitioner:     partitioner,
	}
	d.isShuffle = true
	return d
}

type _NarrowDependency struct {
	*_BaseDependency
	isShuffle bool
}

func newNarrowDependency(rdd RDD) *_NarrowDependency {
	return &_NarrowDependency{
		_BaseDependency: newBaseDependency(rdd),
		isShuffle:       false,
	}
}

type _RangeDependency struct {
	*_NarrowDependency
	inStart  int
	outStart int
	length   int
}

func newRangeDependency(rdd RDD, inStart, outStart, length int) *_RangeDependency {
	d := &_RangeDependency{
		_NarrowDependency: newNarrowDependency(rdd),
		inStart:           inStart,
		outStart:          outStart,
		length:            length,
	}
	return d
}

func (d *_RangeDependency) getParents(pid int) []int {
	if pid >= d.outStart && pid < (d.outStart+d.length) {
		val := pid - d.outStart + d.inStart
		return []int{val}
	}
	return []int{}
}

type _OneToOneDependency struct {
	*_NarrowDependency
}

func newOneToOneDependency(rdd RDD) *_OneToOneDependency {
	d := &_OneToOneDependency{
		_NarrowDependency: newNarrowDependency(rdd),
	}
	return d
}

func (d *_OneToOneDependency) getParents(pid int) []int {
	return []int{pid}
}
