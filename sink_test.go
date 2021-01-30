package main

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func Test_One(t *testing.T) {
	sink := newRecordSink()

	g := new(errgroup.Group)

	g.Go(func() error {
		r, err := sink.add(record{
			Name: "first",
		})

		require.NotEmpty(t, r)
		require.NotZero(t, r.ID)

		return err
	})

	err := g.Wait()

	require.NoError(t, err)
	require.Len(t, sink.recordMap, 1)
	require.EqualValues(t, "first", sink.recordMap[1].Name)
}

func Test_Two(t *testing.T) {
	sink := newRecordSink()

	g := new(errgroup.Group)

	g.Go(func() error {
		r, err := sink.add(record{
			Name: "first",
		})

		require.NotEmpty(t, r)
		require.NotZero(t, r.ID)

		return err
	})

	g.Go(func() error {
		r, err := sink.add(record{
			Name: "second",
		})

		require.NotEmpty(t, r)
		require.NotZero(t, r.ID)

		return err
	})

	err := g.Wait()

	require.NoError(t, err)
	require.Len(t, sink.recordMap, 2)
	require.NotEqualValues(t, sink.recordMap[1].Name, sink.recordMap[2].Name)
}

func Test_TenThousand(t *testing.T) {
	sink := newRecordSink()

	g := new(errgroup.Group)

	for i := 1; i < 100; i++ {
		z := i
		g.Go(func() error {
			r, err := sink.add(record{
				Name: strconv.Itoa(z),
			})

			require.NotEmpty(t, r)
			require.NotZero(t, r.ID)

			return err
		})
	}

	err := g.Wait()

	require.NoError(t, err)
}
