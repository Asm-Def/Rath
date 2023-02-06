import React, { useCallback, useMemo, useRef } from 'react';
import { observer } from 'mobx-react-lite';
import { ActionButton, CommandButton, DefaultButton, IContextualMenuProps } from '@fluentui/react';
import intl from 'react-intl-universal';
import { IFieldMeta, IVisSpecType } from '../../../interfaces';
import { useGlobalStore } from '../../../store';
import ViewField from '../../megaAutomation/vizOperation/viewField';
import FieldPlaceholder from '../../../components/fieldPill/fieldPlaceholder';
import { MainViewContainer } from '../components';
import FilterCreationPill from '../../../components/fieldPill/filterCreationPill';
import Narrative from '../narrative';
import EncodeCreationPill from '../../../components/fieldPill/encodeCreationPill';
import EditorCore from '../../editor/core/index';
import type { IReactVegaHandler } from '../../../components/react-vega';
import MainCanvas from './mainCanvas';
import MiniFloatCanvas from './miniFloatCanvas';

const BUTTON_STYLE = { marginRight: '1em', marginTop: '1em' };

const FocusZone: React.FC = () => {
    const { semiAutoStore, commonStore, collectionStore, painterStore, editorStore } = useGlobalStore();
    const { mainVizSetting, mainView, showMiniFloatView, mainViewSpec, fieldMetas, neighborKeys, mainViewSpecSource } = semiAutoStore;
    const { muteSpec } = editorStore;
    const appendFieldHandler = useCallback(
        (fid: string) => {
            if (mainView === null) {
                semiAutoStore.initMainViewWithSingleField(fid);
            } else {
                semiAutoStore.addMainViewField(fid);
            }
        },
        [semiAutoStore, mainView]
    );

    const editChart = useCallback(() => {
        if (mainViewSpec) {
            commonStore.visualAnalysisInGraphicWalker(mainViewSpec);
        }
    }, [mainViewSpec, commonStore]);

    const paintChart = useCallback(() => {
        if (mainViewSpec && mainView) {
            painterStore.analysisInPainter(mainViewSpec, mainView);
        }
    }, [mainViewSpec, painterStore, mainView]);

    const viewSpec = useMemo(() => {
        return mainViewSpecSource === 'custom' ? muteSpec : mainViewSpec;
    }, [mainViewSpec, muteSpec, mainViewSpecSource]);

    const ChartEditButtonProps = useMemo<IContextualMenuProps>(() => {
        return {
            items: [
                {
                    key: 'editingInGW',
                    text: intl.get('megaAuto.commandBar.editInGW'),
                    iconProps: { iconName: 'BarChartVerticalEdit' },
                    onClick: editChart,
                },
                {
                    key: 'editingInEditor',
                    text: intl.get('megaAuto.commandBar.editInEditor'),
                    iconProps: { iconName: 'Edit' },
                    onClick: () => {
                        if (mainViewSpec) {
                            editorStore.syncSpec(IVisSpecType.vegaSubset, mainViewSpec);
                            semiAutoStore.changeMainViewSpecSource();
                        }
                    },
                },
            ],
        };
    }, [editChart, editorStore, mainViewSpec, semiAutoStore]);

    const handler = useRef<IReactVegaHandler>(null);

    return (
        <MainViewContainer>
            {mainView && showMiniFloatView && <MiniFloatCanvas pined={mainView} />}
            <div className="vis-container">
                <div className="spec">
                    {mainViewSpecSource === 'custom' && (
                        <EditorCore
                            actionPosition="bottom"
                            actionButtons={
                                <DefaultButton
                                    text={intl.get('megaAuto.exitEditor')}
                                    onClick={() => {
                                        semiAutoStore.setMainViewSpecSource('default');
                                    }}
                                />
                            }
                        />
                    )}
                </div>
                <div className="vis">{mainView && mainViewSpec && <MainCanvas handler={handler} view={mainView} spec={viewSpec} />}</div>
                {mainVizSetting.nlg && (
                    <div style={{ overflow: 'auto' }}>
                        <Narrative />
                    </div>
                )}
            </div>
            <hr style={{ marginTop: '1em' }} />
            <div className="fields-container">
                {mainView &&
                    mainView.fields.map((f: IFieldMeta) => (
                        <ViewField
                            onDoubleClick={() => {
                                semiAutoStore.setNeighborKeys(neighborKeys.includes(f.fid) ? [] : [f.fid]);
                            }}
                            mode={neighborKeys.includes(f.fid) ? 'wildcard' : 'real'}
                            key={f.fid}
                            type={f.analyticType}
                            text={f.name || f.fid}
                            onRemove={() => {
                                semiAutoStore.removeMainViewField(f.fid);
                            }}
                        />
                    ))}
                <FieldPlaceholder fields={fieldMetas} onAdd={appendFieldHandler} />
            </div>
            <div className="fields-container">
                {mainView &&
                    mainView.filters &&
                    mainView.filters.map((f) => {
                        const targetField = fieldMetas.find((m) => m.fid === f.fid);
                        if (!targetField) return null;
                        let filterDesc = `${targetField.name || targetField.fid} ∈ `;
                        filterDesc += f.type === 'range' ? `[${f.range.join(',')}]` : `{${f.values.join(',')}}`;
                        return (
                            <ViewField
                                key={f.fid}
                                type={targetField.analyticType}
                                text={filterDesc}
                                onRemove={() => {
                                    semiAutoStore.removeMainViewFilter(f.fid);
                                }}
                            />
                        );
                    })}
                <FilterCreationPill
                    fields={fieldMetas}
                    onFilterSubmit={(field, filter) => {
                        semiAutoStore.addMainViewFilter(filter);
                    }}
                />
            </div>
            <div className="fields-container">
                {mainView &&
                    mainView.encodes &&
                    mainView.encodes.map((f) => {
                        if (f.field === undefined)
                            return (
                                <ViewField
                                    key={'_'}
                                    type="measure"
                                    text="count"
                                    onRemove={() => {
                                        semiAutoStore.removeFieldEncodeFromMainViewPattern(f);
                                    }}
                                />
                            );
                        const targetField = fieldMetas.find((m) => m.fid === f.field);
                        if (!targetField) return null;
                        let filterDesc = `${f.aggregate}(${targetField.name || targetField.fid})`;
                        return (
                            <ViewField
                                key={f.field}
                                type={targetField.analyticType}
                                text={filterDesc}
                                onRemove={() => {
                                    semiAutoStore.removeFieldEncodeFromMainViewPattern(f);
                                }}
                            />
                        );
                    })}
                {mainView && (
                    <EncodeCreationPill
                        fields={mainView.fields}
                        onSubmit={(encode) => {
                            semiAutoStore.addFieldEncode2MainViewPattern(encode);
                        }}
                    />
                )}
            </div>
            <div className="action-buttons">
                <CommandButton
                    style={BUTTON_STYLE}
                    text={intl.get('megaAuto.commandBar.editing')}
                    iconProps={{ iconName: 'BarChartVerticalEdit' }}
                    disabled={mainView === null}
                    menuProps={ChartEditButtonProps}
                />
                <ActionButton
                    style={BUTTON_STYLE}
                    text={intl.get('megaAuto.commandBar.painting')}
                    iconProps={{ iconName: 'EditCreate' }}
                    disabled={mainView === null}
                    onClick={paintChart}
                />
                {mainView && mainViewSpec && (
                    <ActionButton
                        style={BUTTON_STYLE}
                        iconProps={{
                            iconName: collectionStore.collectionContains(fieldMetas, mainViewSpec, IVisSpecType.vegaSubset, mainView.filters)
                                ? 'FavoriteStarFill'
                                : 'FavoriteStar',
                        }}
                        text={intl.get('common.star')}
                        onClick={() => {
                            collectionStore.toggleCollectState(fieldMetas, mainViewSpec, IVisSpecType.vegaSubset, mainView.filters);
                        }}
                    />
                )}
                <ActionButton
                    style={BUTTON_STYLE}
                    iconProps={{ iconName: 'Settings' }}
                    ariaLabel={intl.get('common.settings')}
                    title={intl.get('common.settings')}
                    text={intl.get('common.settings')}
                    onClick={() => {
                        semiAutoStore.setShowSettings(true);
                    }}
                />
                <ActionButton
                    style={{ marginTop: BUTTON_STYLE.marginTop }}
                    iconProps={{ iconName: 'Download' }}
                    ariaLabel={intl.get('megaAuto.commandBar.download')}
                    title={intl.get('megaAuto.commandBar.download')}
                    text={intl.get('megaAuto.commandBar.download')}
                    disabled={mainView === null}
                    onClick={() => {
                        handler.current?.exportImage();
                    }}
                />
            </div>
        </MainViewContainer>
    );
};

export default observer(FocusZone);
