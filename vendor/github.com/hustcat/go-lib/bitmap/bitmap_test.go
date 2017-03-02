// Copyright 2014 bitmap authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Author：Ye Yin<hustcat@gmail.com>

package bitmap

import (
	"fmt"
	"testing"
)

func TestBitmapSet(t *testing.T) {
	bitmap := NewNumaBitmap()
	bitmap.SetBit(1, 1)
	bitmap.SetBit(10, 1)
	actual := bitmap.String()
	expected := "[1 10]"
	if expected != actual {
		t.Errorf("expected:%s, actual:%s", expected, actual)
	}
}

func TestGet1BitOffs(t *testing.T) {
	bitmap := NewNumaBitmap()
	bitmap.SetBit(1, 1)
	bitmap.SetBit(10, 1)
	bitmap.SetBit(15, 1)
	bitmap.SetBit(10, 0)
	bitmap.SetBit(10, 1)
	actual := bitmap.Get1BitOffs()
	expected := []uint{1, 10, 15}
	a := fmt.Sprintf("%v", expected)
	b := fmt.Sprintf("%v", actual)
	if a != b {
		t.Errorf("expected:%v, actual:%v", a, b)
	}
}

func TestGet1BitOffsNuma(t *testing.T) {
	bitmap := NewNumaBitmap()

	//node 0
	bitmap.SetBit(0, 1)
	bitmap.SetBit(5, 1)

	//node 1
	bitmap.SetBit(6, 1)
	bitmap.SetBit(11, 1)

	//node 0
	bitmap.SetBit(12, 1)
	bitmap.SetBit(17, 1)

	//node 1
	bitmap.SetBit(18, 1)
	bitmap.SetBit(23, 1)

	actual, _ := bitmap.Get1BitOffsNuma(2)
	expected := [][]uint{
		[]uint{0, 5, 12, 17},
		[]uint{6, 11, 18, 23},
	}
	a := fmt.Sprintf("%v", expected)
	b := fmt.Sprintf("%v", actual)
	if a != b {
		t.Errorf("expected:%v, actual:%v", a, b)
	}
}

func TestGet1BitOffsNuma4(t *testing.T) {
	bitmap := NewNumaBitmapSize(48, 4)

	//node 0
	bitmap.SetBit(0, 1)
	bitmap.SetBit(5, 1)

	//node 1
	bitmap.SetBit(6, 1)
	bitmap.SetBit(11, 1)

	//node 2
	bitmap.SetBit(12, 1)
	bitmap.SetBit(17, 1)

	//node 3
	bitmap.SetBit(18, 1)
	bitmap.SetBit(23, 1)

	//node 0
	bitmap.SetBit(24, 1)
	bitmap.SetBit(29, 1)

	//node 1
	bitmap.SetBit(30, 1)
	bitmap.SetBit(35, 1)

	//node 2
	bitmap.SetBit(36, 1)
	bitmap.SetBit(41, 1)

	//node 3
	bitmap.SetBit(42, 1)
	bitmap.SetBit(47, 1)

	actual, _ := bitmap.Get1BitOffsNuma(4)
	expected := [][]uint{
		[]uint{0, 5, 24, 29},
		[]uint{6, 11, 30, 35},
		[]uint{12, 17, 36, 41},
		[]uint{18, 23, 42, 47},
	}
	a := fmt.Sprintf("%v", expected)
	b := fmt.Sprintf("%v", actual)
	if a != b {
		t.Errorf("expected:%v, actual:%v", a, b)
	}
}
