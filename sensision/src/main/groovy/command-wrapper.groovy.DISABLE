//
//   Copyright 2016  Cityzen Data
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

//
// @see http://groovy.codehaus.org/Process+Management
//

//
// Run the desired process
//

def p = "ls -lart".execute()

//
// Consume its output, ignoring results
//

def out = new StringBuffer()
def err = new StringBuffer()

p.consumeProcessOutput(out,err)

if(err.size() > 0) println err
