// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bstream

// Preprocessor will run a preprocess func only if `obj` is empty or if it matches a ForkableObject
// where the WrappedObject() is nil
type Preprocessor struct {
	preprocFunc PreprocessFunc
	handler     Handler
}

func NewPreprocessor(preprocFunc PreprocessFunc, next Handler) *Preprocessor {
	return &Preprocessor{
		preprocFunc: preprocFunc,
		handler:     next,
	}
}

func (p *Preprocessor) ProcessBlock(blk *Block, obj interface{}) (err error) {
	if obj == nil {
		obj, err = p.preprocFunc(blk)
		if err != nil {
			return err
		}
	}
	if forkableObj, ok := obj.(ForkableObject); ok {
		if wrappedObj := forkableObj.WrappedObject(); wrappedObj == nil {
			newWrappedObj, err := p.preprocFunc(blk)
			if err != nil {
				return err
			}
			obj = &reprocessedForkableObject{
				step:   forkableObj.Step(),
				cursor: forkableObj.Cursor(),
				obj:    newWrappedObj,
			}
		}
	}
	return p.handler.ProcessBlock(blk, obj)
}

type reprocessedForkableObject struct {
	cursor *Cursor
	step   StepType
	obj    interface{}
}

func (fobj *reprocessedForkableObject) Step() StepType {
	return fobj.step
}

func (fobj *reprocessedForkableObject) WrappedObject() interface{} {
	return fobj.obj
}

func (fobj *reprocessedForkableObject) Cursor() *Cursor {
	return fobj.cursor
}
