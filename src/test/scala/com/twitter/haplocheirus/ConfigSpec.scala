/*
 * Copyright 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.haplocheirus

import java.io.File
import com.twitter.util.Eval
import org.specs.Specification

object ConfigSpec extends Specification {
  "production config" should {
    "eval" in {
      try {
        val config = Eval[com.twitter.haplocheirus.HaplocheirusConfig](new File("config/production.scala"))
        config mustNot beNull
      } catch {
        case e: Throwable => e.printStackTrace()
        throw e
      }
    }
  }

  "test config" should {
    "eval" in {
      try {
        val config = Eval[com.twitter.haplocheirus.HaplocheirusConfig](new File("config/test.scala"))
        config mustNot beNull
      } catch {
        case e: Throwable => e.printStackTrace()
        throw e
      }
    }
  }
}

