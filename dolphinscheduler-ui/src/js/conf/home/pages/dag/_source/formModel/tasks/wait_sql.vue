/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
<template>
  <div class="wait_sql-model">
    <m-list-box>
      <div slot="text">{{$t('Datasource')}}</div>
      <div slot="content">
        <m-datasource
          ref="refDs"
          @on-dsData="_onDsData"
          :data="{ type:type,datasource:datasource }">
        </m-datasource>
      </div>
    </m-list-box>

    <m-list-box v-show="type === 'HIVE'">
      <div slot="text">{{$t('SQL Parameter')}}</div>
      <div slot="content">
        <x-input
          :disabled="isDetails"
          type="input"
          v-model="connParams"
          :placeholder="$t('Please enter format') + ' key1=value1;key2=value2...'"
          autocomplete="off">
        </x-input>
      </div>
    </m-list-box>
    <m-list-box>
      <div slot="text">
        <span>{{$t('SQL Statement')}}</span>
        <span>({{$t('Wait_sql  result 1 is success')}})</span>
      </div>
      <div slot="content">
        <div class="from-mirror">
          <textarea
            id="code-sql-mirror"
            name="code-sql-mirror"
            style="opacity: 0;">
          </textarea>
        </div>

      </div>
    </m-list-box>
    <m-list-box v-if="type === 'HIVE'">
      <div slot="text">{{$t('UDF Function')}}</div>
      <div slot="content">
        <m-udfs
          ref="refUdfs"
          @on-udfsData="_onUdfsData"
          :udfs="udfs"
          :type="type">
        </m-udfs>
      </div>
    </m-list-box>

    <m-list-box>
      <div slot="text">
        <span>{{$t('Wait_sql look interval')}}</span>
      </div>
      <div slot="content">
        <m-select-input v-model="lookInterval" :list="[1,2,5,10,15]">
        </m-select-input>
        <span>({{$t('Minute')}})</span>
      </div>
    </m-list-box>
    <m-list-box>
      <div slot="text">{{$t('Custom Parameters')}}</div>
      <div slot="content">
        <m-local-params
          ref="refLocalParams"
          @on-udpData="_onUdpData"
          :udp-list="localParams">
        </m-local-params>
      </div>
    </m-list-box>

  </div>
</template>
<script>
  import _ from 'lodash'
  import i18n from '@/module/i18n'
  import mUdfs from './_source/udfs'
  import mListBox from './_source/listBox'
  import mDatasource from './_source/datasource'
  import mLocalParams from './_source/localParams'
  import disabledState from '@/module/mixin/disabledState'
  import codemirror from '@/conf/home/pages/resource/pages/file/pages/_source/codemirror'
  import mSelectInput from '../_source/selectInput'

  let editor

  export default {
    name: 'wait_sql',
    data () {
      return {
        // Data source type
        type: '',
        // data source
        datasource: '',
        // Return to the selected data source
        rtDatasource: '',
        // Sql statement
        sql: '',
        // Custom parameter
        localParams: [],
        // UDF function
        udfs: '',
        // Sql parameter
        connParams: '',
        //Wait_sql look interval
        lookInterval: '1'

      }
    },
    mixins: [disabledState],
    props: {
      backfillItem: Object,
      createNodeId: Number
    },
    methods: {

      /**
       * return udfs
       */
      _onUdfsData (a) {
        this.udfs = a
      },
      /**
       * return Custom parameter
       */
      _onUdpData (a) {
        this.localParams = a
      },
      /**
       * return data source
       */
      _onDsData (o) {
        this.type = o.type
        this.rtDatasource = o.datasource
      },
      /**
       * verification
       */
      _verification () {
        if (!editor.getValue()) {
          this.$message.warning(`${i18n.$t('Please enter a SQL Statement(required)')}`)
          return false
        }
        // datasource Subcomponent verification
        if (!this.$refs.refDs._verifDatasource()) {
          return false
        }
        // udfs Subcomponent verification Verification only if the data type is HIVE
        if (this.type === 'HIVE') {
          if (!this.$refs.refUdfs._verifUdfs()) {
            return false
          }
        }

        // localParams Subcomponent verification
        if (!this.$refs.refLocalParams._verifProp()) {
          return false
        }

        // Verify lookInterval duration Non 0 positive integer
        if (!parseInt(this.lookInterval) && !_.isInteger(this.lookInterval)) {
          this.$message.warning(`${i18n.$t('Timeout must be a positive integer')}`)
          return false
        }

        // storage
        this.$emit('on-params', {
          type: this.type,
          datasource: this.rtDatasource,
          sql: editor.getValue(),
          udfs: this.udfs,
          localParams: this.localParams,
          connParams: this.connParams,
          lookInterval: this.lookInterval
        })
        return true
      },
      /**
       * Processing code highlighting
       */
      _handlerEditor () {
        this._destroyEditor()

        // editor
        editor = codemirror('code-sql-mirror', {
          mode: 'sql',
          readOnly: this.isDetails
        })

        this.keypress = () => {
          if (!editor.getOption('readOnly')) {
            editor.showHint({
              completeSingle: false
            })
          }
        }

        this.changes = () => {
          this._cacheParams()
        }

        // Monitor keyboard
        editor.on('keypress', this.keypress)

        editor.on('changes', this.changes)

        editor.setValue(this.sql)

        return editor
      },
      _getReceiver () {
        let param = {}
        let current = this.router.history.current
        if (current.name === 'projects-definition-details') {
          param.processDefinitionId = current.params.id
        } else {
          param.processInstanceId = current.params.id
        }
      },
      _cacheParams () {
        this.$emit('on-cache-params', {
          type: this.type,
          datasource: this.rtDatasource,
          sql: editor ? editor.getValue() : '',
          udfs: this.udfs,
          localParams: this.localParams,
          connParams: this.connParams,
          lookInterval: this.lookInterval
        });
      },
      _destroyEditor () {
        if (editor) {
          editor.toTextArea() // Uninstall
          editor.off($('.code-sql-mirror'), 'keypress', this.keypress)
          editor.off($('.code-sql-mirror'), 'changes', this.changes)
        }
      }
    },
    watch: {
      // Listening data source
      type (val) {
        if (val !== 'HIVE') {
          this.connParams = ''
        }
      },
      //Watch the cacheParams
      cacheParams (val) {
        this._cacheParams()
      }
    },
    created () {

      let o = this.backfillItem
      // Non-null objects represent backfill
      if (!_.isEmpty(o)) {
        // backfill
        this.type = o.params.type || ''
        this.datasource = o.params.datasource || ''
        this.sql = o.params.sql || ''
        this.udfs = o.params.udfs || ''
        this.connParams = o.params.connParams || ''
        this.localParams = o.params.localParams || []
        this.lookInterval = o.params.lookInterval || ''
      }else {
        // Timeout Setting
        this.$parent.$refs["timeout"]._changTime()
      }
      // read tasks from cache
      if (!_.some(this.store.state.dag.cacheTasks, { id: this.createNodeId }) &&
        this.router.history.current.name !== 'definition-create') {
        this._getReceiver()
      }
    },
    mounted () {
      setTimeout(() => {
        this._handlerEditor()
      }, 200)
    },
    destroyed () {
      /**
       * Destroy the editor instance
       */
      if (editor) {
        editor.toTextArea() // Uninstall
        editor.off($('.code-sql-mirror'), 'keypress', this.keypress)
        editor.off($('.code-sql-mirror'), 'changes', this.changes)
      }
    },
    computed: {
      cacheParams () {
        return {
          type: this.type,
          datasource: this.rtDatasource,
          udfs: this.udfs,

          localParams: this.localParams,
          connParams: this.connParams,
          lookInterval: this.lookInterval

        }
      }
    },
    components: { mListBox, mDatasource, mLocalParams, mUdfs, mSelectInput }
  }
</script>
<style lang="scss" rel="stylesheet/scss">
  .requiredIcon {
    color: #ff0000;
    padding-right: 4px;
  }
</style>

